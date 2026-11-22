# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Deep reconciliation: Azure SQL ↔ bronze
# MAGIC
# MAGIC Goes beyond row counts. For each table, it proves the latest bronze copy matches Azure SQL **value for value**:
# MAGIC
# MAGIC 1. **Profile:** null counts, sums, min/max dates and distinct text values, per column.
# MAGIC 2. **Fingerprints:** every row hashed (SHA-256) on both sides; rows split into 64 buckets; each bucket's
# MAGIC    row count and fingerprint sum must match.
# MAGIC 3. **Drill-down:** only for buckets that differ: exactly which rows are missing, extra or changed, and which
# MAGIC    columns. Results go to `audit.recon_diffs`; PII values are hidden.
# MAGIC
# MAGIC Every check is logged to `audit.gate_results` as gate `deep_recon_source_to_bronze`.
# MAGIC The last section **breaks a copy on purpose** to show the recon finding the exact problem.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./quality_recon

# COMMAND ----------

import uuid
from datetime import datetime, timezone

RUN_ID = str(uuid.uuid4())
BUCKETS = 64
TABLES = [
    # (source table, bronze table, key, PII columns whose values are never written to the diff table)
    ("brokerage.advisors", "advisors", "advisor_id", ("email",)),
    ("brokerage.clients",  "clients",  "client_id",  ("email", "phone", "ssn", "first_name", "last_name", "date_of_birth")),
    ("brokerage.accounts", "accounts", "account_id", ("account_number",)),
    ("brokerage.cash_ledger", "cash_ledger", "cash_txn_id", ()),
]

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.audit.recon_diffs (
    run_id STRING, table_name STRING, key_value STRING,
    issue STRING        COMMENT 'missing_in_target, extra_in_target or changed',
    column_name STRING  COMMENT 'for changed rows: the column that differs',
    source_value STRING, target_value STRING  COMMENT 'PII values shown as ***',
    found_at TIMESTAMP
) COMMENT 'Row- and column-level differences found by deep reconciliation'
""")


def latest_bronze(table):
    bronze = spark.table(f"{CATALOG}.bronze.{table}")
    last = bronze.agg(F.max_by("_run_id", "_ingested_at")).first()[0]
    return bronze.filter(F.col("_run_id") == last)


def business_columns(df):
    return [c for c in df.columns if not c.startswith("_")]


def gate_row(table, check, sev, exp, act, ok):
    return (RUN_ID, "deep_recon_source_to_bronze", f"bronze.{table}", check, sev,
            str(exp), str(act), "PASS" if ok else "FAIL", datetime.now(timezone.utc))


def deep_recon(source_table, target, table_label, key, pii, results):
    cols = business_columns(target)

    # 1 · Profile
    profile = profile_compare(JDBC, source_table, target, cols)
    bad = [p for p in profile if not p[4]]
    results.append(gate_row(table_label, "column_profile_matches", "error",
                            f"{len(profile)} metrics", f"{len(bad)} differ", not bad))
    for col, metric, s, t, _ in bad:
        print(f"   ✗ {table_label}.{col} {metric}: source={s} target={t}")

    # 2 · Fingerprints
    bad_buckets, n = bucket_compare(JDBC, source_table, target, key, cols, BUCKETS)
    results.append(gate_row(table_label, "row_fingerprints_match", "error",
                            f"{n} buckets match", f"{len(bad_buckets)} differ", not bad_buckets))

    # 3 · Drill-down (only if something differs)
    diffs = drill_down(JDBC, source_table, target, key, cols, bad_buckets, n, pii=pii)
    if diffs is not None:
        diffs = diffs.select(F.lit(RUN_ID).alias("run_id"), F.lit(table_label).alias("table_name"), "*",
                             F.current_timestamp().alias("found_at"))
        display(diffs)
    status = "✅" if not bad and not bad_buckets else "❌"
    print(f"{status} {table_label}: {len(profile)} column metrics, {n} fingerprint buckets, "
          f"{len(bad_buckets)} bucket(s) differ")
    return diffs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run it on the latest bronze copy

# COMMAND ----------

results, all_diffs = [], []
for source_table, table, key, pii in TABLES:
    d = deep_recon(source_table, latest_bronze(table), table, key, pii, results)
    if d is not None:
        all_diffs.append(d)

for d in all_diffs:
    d.write.mode("append").saveAsTable(f"{CATALOG}.audit.recon_diffs")

gate = spark.createDataFrame(results, spark.table(f"{CATALOG}.audit.gate_results").schema)
gate.write.mode("append").saveAsTable(f"{CATALOG}.audit.gate_results")
display(gate)
assert all(r[7] == "PASS" for r in results), "❌ Deep reconciliation found differences (see audit.recon_diffs)"
print("✅ Bronze matches Azure SQL value for value")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo: break a copy on purpose
# MAGIC Works on an **in-memory copy**; nothing is written to bronze or the audit tables.
# MAGIC Three problems are planted: one client's state changed, one client removed, one client duplicated with a new id.
# MAGIC The recon should find exactly those three rows, and say the changed column is `state`.

# COMMAND ----------

clients = latest_bronze("clients")
first_ids = [r.client_id for r in clients.select("client_id").orderBy("client_id").limit(3).collect()]
changed_id, removed_id, copied_id = first_ids

broken = (clients
          .withColumn("state", F.when(F.col("client_id") == changed_id, F.lit("ZZ")).otherwise(F.col("state")))
          .where(F.col("client_id") != removed_id)
          .unionByName(clients.where(F.col("client_id") == copied_id)
                       .withColumn("client_id", F.lit(999_999))))

demo_results = []
demo = deep_recon("brokerage.clients", broken, "clients (demo, broken copy)", "client_id",
                  ("email", "phone", "ssn", "first_name", "last_name", "date_of_birth"), demo_results)
print(f"Planted: changed {changed_id} (state), removed {removed_id}, extra 999999")
