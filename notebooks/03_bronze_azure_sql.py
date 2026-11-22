# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Bronze: copy Azure SQL → `bronze` (slice 1)
# MAGIC
# MAGIC **Bronze is a faithful copy of the source**, plus three lineage labels on every row:
# MAGIC
# MAGIC | Column | Meaning |
# MAGIC |---|---|
# MAGIC | `_source_system` | where the row came from (`azure_sql`) |
# MAGIC | `_ingested_at` | when it was copied (UTC) |
# MAGIC | `_run_id` | which pipeline run copied it |
# MAGIC
# MAGIC Every run **appends** a full copy, so bronze keeps a history of what the source looked like each time.
# MAGIC
# MAGIC **Gate: source → bronze.** Azure SQL's own numbers at copy time must equal what landed in bronze:
# MAGIC - **row count** catches missing or duplicated rows;
# MAGIC - **key sum** (sum of all primary-key values) catches a count that matches while the rows differ.
# MAGIC
# MAGIC Results go to `audit.gate_results`. If any check fails, the notebook stops, so silver never runs on a bad copy.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F

RUN_ID = str(uuid.uuid4())                 # one id for this whole run
READ_PARTITIONS = 4                        # parallel connections to Azure SQL (free tier = 2 vCores)
SOURCES = [("advisors", "advisor_id"),     # (table, primary key)
           ("clients",  "client_id"),
           ("accounts", "account_id"),
           ("cash_ledger", "cash_txn_id")]

print("Run:", RUN_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy each table into bronze (parallel read)

# COMMAND ----------

source_stats = {}

for table, key in SOURCES:
    # 1) Azure SQL's own numbers at copy time: the "expected" side of the reconciliation
    stats = (spark.read.format("jdbc").options(**JDBC)
             .option("query", f"""SELECT COUNT(*)                   AS n,
                                         SUM(CAST({key} AS BIGINT)) AS key_sum,
                                         MIN({key})                 AS lo,
                                         MAX({key})                 AS hi
                                  FROM brokerage.{table}""")
             .load().first())
    source_stats[table] = stats

    # 2) Read the table in parallel slices of the primary key
    df = (spark.read.format("jdbc").options(**JDBC)
          .option("dbtable", f"brokerage.{table}")
          .option("partitionColumn", key)
          .option("lowerBound", stats["lo"])
          .option("upperBound", stats["hi"])
          .option("numPartitions", READ_PARTITIONS)
          .load())

    # 3) Add lineage labels and append to bronze
    (df.withColumn("_source_system", F.lit("azure_sql"))
       .withColumn("_ingested_at", F.current_timestamp())
       .withColumn("_run_id", F.lit(RUN_ID))
       .write.mode("append")
       .option("mergeSchema", "true")
       .saveAsTable(f"{CATALOG}.bronze.{table}"))
    print(f"⬆️  {CATALOG}.bronze.{table}: source has {stats['n']:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gate: source → bronze

# COMMAND ----------

results = []
for table, key in SOURCES:
    expected = source_stats[table]
    landed = (spark.table(f"{CATALOG}.bronze.{table}")
              .filter(F.col("_run_id") == RUN_ID)              # only this run's copy
              .agg(F.count("*").alias("n"),
                   F.sum(F.col(key).cast("bigint")).alias("key_sum"))
              .first())

    for check, exp, act in [("row_count_matches_source", expected["n"], landed["n"]),
                            ("key_sum_matches_source", expected["key_sum"], landed["key_sum"])]:
        results.append((RUN_ID, "source_to_bronze", f"bronze.{table}", check, "error",
                        str(exp), str(act), "PASS" if exp == act else "FAIL",
                        datetime.now(timezone.utc)))

gate = spark.createDataFrame(results, spark.table(f"{CATALOG}.audit.gate_results").schema)
gate.write.mode("append").saveAsTable(f"{CATALOG}.audit.gate_results")
display(gate)

failed = [r for r in results if r[7] == "FAIL"]
assert not failed, f"❌ Gate source_to_bronze FAILED: {len(failed)} check(s). Silver must not run."
print("✅ Gate source_to_bronze passed")