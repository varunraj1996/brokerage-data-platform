# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Silver: clean and validate Azure SQL data (slice 1)
# MAGIC
# MAGIC Takes the **latest bronze copy** of advisors, clients and accounts and produces **clean, trusted** silver tables.
# MAGIC
# MAGIC | Step | What happens |
# MAGIC |---|---|
# MAGIC | 1 · Standardise | trim text, lower-case emails, upper-case state codes |
# MAGIC | 2 · Deduplicate | keep one row per primary key (the most recently ingested) |
# MAGIC | 3 · Validate | business rules + foreign keys; failing rows go to **quarantine** with a reason, never silently dropped |
# MAGIC | 4 · Gate | rows in = rows out + duplicates + rejects; keys unique; too many rejects → stop |
# MAGIC | 5 · Publish | `MERGE` the valid rows into silver (insert new, update changed, remove rows gone from the source) |
# MAGIC
# MAGIC This is **Write → Audit → Publish**: silver is only changed after the checks pass.
# MAGIC Tables run parents first (advisors → clients → accounts) so foreign keys can be checked against silver.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import uuid
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

RUN_ID = str(uuid.uuid4())
MAX_REJECT_RATE = 0.01          # above 1% rejected rows the gate FAILs; any rejects at all = WARN

print("Run:", RUN_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rules
# MAGIC Each rule is a SQL condition that must be **true** for a good row. A row failing any rule is quarantined, with every
# MAGIC failed rule listed as its reason. Foreign keys are checked against the parent's **silver** table.

# COMMAND ----------

TABLES = [
    {
        "name": "advisors", "key": "advisor_id",
        "rules": {
            "advisor_id_not_null": "advisor_id IS NOT NULL",
            "email_valid":         "email RLIKE '^[^@\\\\s]+@[^@\\\\s]+\\\\.[a-z]+$'",
            "hired_not_in_future": "hired_date <= current_date()",
        },
        "foreign_keys": {},
    },
    {
        "name": "clients", "key": "client_id",
        "rules": {
            "client_id_not_null":      "client_id IS NOT NULL",
            "email_valid":             "email RLIKE '^[^@\\\\s]+@[^@\\\\s]+\\\\.[a-z]+$'",
            "ssn_format":              "ssn RLIKE '^[0-9]{3}-[0-9]{2}-[0-9]{4}$'",
            "risk_profile_valid":      "risk_profile IN ('Conservative','Moderate','Aggressive')",
            "segment_valid":           "segment IN ('Mass','Affluent','HNW')",
            "born_before_onboarding":  "date_of_birth < onboarded_date",
        },
        "foreign_keys": {"advisor_id": ("advisors", "advisor_id")},
    },
    {
        "name": "accounts", "key": "account_id",
        "rules": {
            "account_id_not_null":   "account_id IS NOT NULL",
            "account_type_valid":    "account_type IN ('Individual','Joint','IRA','Roth IRA')",
            "status_matches_dates":  "(status = 'Open' AND closed_date IS NULL) OR "
                                     "(status = 'Closed' AND closed_date IS NOT NULL)",
            "closed_after_opened":   "closed_date IS NULL OR closed_date >= opened_date",
        },
        "foreign_keys": {"client_id": ("clients", "client_id")},
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine table
# MAGIC One table for every rejected row from every source: which table, why, and the full original row as JSON.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.audit.quarantine (
    run_id          STRING,
    table_name      STRING,
    reject_reason   STRING  COMMENT 'every rule the row failed, separated by ;',
    record          STRING  COMMENT 'the original row as JSON',
    quarantined_at  TIMESTAMP
)
COMMENT 'Rows rejected on the way from bronze to silver'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

def latest_bronze(table: str):
    """Only the rows from the most recent bronze run of this table."""
    bronze = spark.table(f"{CATALOG}.bronze.{table}")
    last_run = bronze.agg(F.max_by("_run_id", "_ingested_at")).first()[0]
    return bronze.filter(F.col("_run_id") == last_run), last_run


def standardise(df):
    """Trim every text column; lower-case emails; upper-case US state codes."""
    for name, dtype in df.dtypes:
        if dtype == "string" and not name.startswith("_"):
            df = df.withColumn(name, F.trim(F.col(name)))
    if "email" in df.columns:
        df = df.withColumn("email", F.lower("email"))
    if "state" in df.columns:
        df = df.withColumn("state", F.upper("state"))
    return df


def gate_row(table, check, severity, expected, actual, status):
    return (RUN_ID, "bronze_to_silver", f"silver.{table}", check, severity,
            str(expected), str(actual), status, datetime.now(timezone.utc))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean → validate → gate → publish, one table at a time

# COMMAND ----------

all_results = []
published = set()          # tables whose silver was updated in this run

for t in TABLES:
    table, key = t["name"], t["key"]
    silver_name = f"{CATALOG}.silver.{table}"

    # A child can't be checked against a parent that failed its own gate
    blocked_by = [p for p, _ in t["foreign_keys"].values() if p not in published]
    if blocked_by:
        all_results.append(gate_row(table, "parent_tables_published", "error",
                                    ", ".join(blocked_by), "not published", "FAIL"))
        print(f"⛔ silver.{table}: skipped, parent {blocked_by} failed its gate")
        continue

    # 1 · Latest bronze copy, standardised
    df, bronze_run = latest_bronze(table)
    df = standardise(df)
    rows_in = df.count()

    # 2 · Deduplicate on the primary key (keep the most recently ingested row)
    w = Window.partitionBy(key).orderBy(F.col("_ingested_at").desc())
    ranked = df.withColumn("_rn", F.row_number().over(w))
    duplicates = ranked.filter("_rn > 1").count()
    df = ranked.filter("_rn = 1").drop("_rn")

    # 3 · Validate: business rules…
    failed = [F.when(~F.expr(cond) | F.expr(cond).isNull(), F.lit(rule)) for rule, cond in t["rules"].items()]

    # …and foreign keys, checked against the parent's silver table
    for col, (parent, parent_key) in t["foreign_keys"].items():
        parent_keys = (spark.table(f"{CATALOG}.silver.{parent}")
                       .select(F.col(parent_key).alias(f"_fk_{col}")).distinct())
        df = df.join(parent_keys, df[col] == parent_keys[f"_fk_{col}"], "left")
        failed.append(F.when(F.col(f"_fk_{col}").isNull(), F.lit(f"{col}_not_in_{parent}")))

    df = df.withColumn("_reject_reason", F.concat_ws("; ", *failed))
    df = df.drop(*[c for c in df.columns if c.startswith("_fk_")])

    valid = df.filter(F.col("_reject_reason") == "").drop("_reject_reason")
    rejects = df.filter(F.col("_reject_reason") != "")
    rows_valid, rows_rejected = valid.count(), rejects.count()

    # Quarantine the rejects (kept, never dropped)
    if rows_rejected:
        record_cols = [c for c in rejects.columns if c != "_reject_reason"]
        (rejects.select(F.lit(RUN_ID).alias("run_id"), F.lit(table).alias("table_name"),
                        F.col("_reject_reason").alias("reject_reason"),
                        F.to_json(F.struct(*record_cols)).alias("record"),
                        F.current_timestamp().alias("quarantined_at"))
         .write.mode("append").saveAsTable(f"{CATALOG}.audit.quarantine"))

    # 4 · Gate (before anything is published)
    reject_rate = rows_rejected / rows_in if rows_in else 0
    results = [
        gate_row(table, "rows_in_equals_out_plus_dupes_plus_rejects", "error",
                 rows_in, rows_valid + duplicates + rows_rejected,
                 "PASS" if rows_in == rows_valid + duplicates + rows_rejected else "FAIL"),
        gate_row(table, "reject_rate", "warn", f"<= {MAX_REJECT_RATE:.0%}", f"{reject_rate:.2%}",
                 "PASS" if rows_rejected == 0 else ("WARN" if reject_rate <= MAX_REJECT_RATE else "FAIL")),
        gate_row(table, "primary_key_unique", "error", rows_valid,
                 valid.select(key).distinct().count(),
                 "PASS" if valid.select(key).distinct().count() == rows_valid else "FAIL"),
    ]
    all_results += results
    if any(r[7] == "FAIL" for r in results):
        print(f"❌ silver.{table}: gate failed, silver NOT updated")
        continue

    # 5 · Publish with MERGE: insert new keys, update existing ones, delete keys no longer in the source
    valid = (valid.withColumnRenamed("_run_id", "_bronze_run_id")
                  .withColumn("_silver_loaded_at", F.current_timestamp()))
    if not spark.catalog.tableExists(silver_name):
        valid.limit(0).write.saveAsTable(silver_name)
    (DeltaTable.forName(spark, silver_name).alias("s")
        .merge(valid.alias("n"), f"s.{key} = n.{key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute())

    published.add(table)
    silver_rows = spark.table(silver_name).count()
    all_results.append(gate_row(table, "silver_rows_equal_valid_rows", "error", rows_valid, silver_rows,
                                "PASS" if silver_rows == rows_valid else "FAIL"))
    print(f"✅ silver.{table}: in {rows_in:,} · valid {rows_valid:,} · duplicates {duplicates:,} · "
          f"rejected {rows_rejected:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Record the gate and stop on failure

# COMMAND ----------

gate = spark.createDataFrame(all_results, spark.table(f"{CATALOG}.audit.gate_results").schema)
gate.write.mode("append").saveAsTable(f"{CATALOG}.audit.gate_results")
display(gate)

failed_checks = [r for r in all_results if r[7] == "FAIL"]
assert not failed_checks, f"❌ Gate bronze_to_silver FAILED: {len(failed_checks)} check(s). dbt must not run."
print("✅ Gate bronze_to_silver passed")