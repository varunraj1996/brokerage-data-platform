# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Set up the lakehouse
# MAGIC
# MAGIC Creates the home for every table, for the chosen environment:
# MAGIC
# MAGIC | Level | Name | Holds |
# MAGIC |---|---|---|
# MAGIC | Catalog | `brokerage_<env>` | the whole project for one environment |
# MAGIC | Schema | `bronze` | raw copies of every source, as received |
# MAGIC | Schema | `silver` | cleaned, typed, deduplicated |
# MAGIC | Schema | `gold` | star schema and marts (built by dbt) |
# MAGIC | Schema | `audit` | quality-gate results, run manifests, quarantine |
# MAGIC | Volume | `bronze.raw` | raw files (trade-event JSON, custodian files) |
# MAGIC
# MAGIC Safe to re-run: everything uses `IF NOT EXISTS`.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")

for schema, about in [("bronze", "raw copies of every source, as received"),
                      ("silver", "cleaned, typed, deduplicated"),
                      ("gold",   "star schema and marts (built by dbt)"),
                      ("audit",  "quality-gate results, run manifests, quarantine")]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema} COMMENT '{about}'")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality-gate results
# MAGIC One table records every check at every layer hop: what was expected, what was found, and whether it passed.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.audit.gate_results (
    run_id      STRING    COMMENT 'one id per pipeline run',
    gate        STRING    COMMENT 'e.g. source_to_bronze',
    table_name  STRING,
    check_name  STRING    COMMENT 'e.g. row_count_matches_source',
    severity    STRING    COMMENT 'error or warn',
    expected    STRING,
    actual      STRING,
    status      STRING    COMMENT 'PASS, WARN or FAIL',
    checked_at  TIMESTAMP
)
COMMENT 'Every quality check at every layer hop'
""")

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

