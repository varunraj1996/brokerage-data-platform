# Databricks notebook source
# MAGIC %md
# MAGIC # 12 · Bronze: stream trade events with Auto Loader
# MAGIC
# MAGIC **Auto Loader** watches the landing folder and loads **only files it hasn't seen before**. It remembers what it
# MAGIC has loaded in a **checkpoint** folder, so every file is loaded exactly once, even across restarts.
# MAGIC
# MAGIC | Setting | Choice | Why |
# MAGIC |---|---|---|
# MAGIC | Schema | fixed (the event contract) | a surprise field must not silently change the table |
# MAGIC | Unexpected fields | kept in `_rescued_data` | nothing is lost; the gate warns about it |
# MAGIC | Trigger | `availableNow` | load everything new, then **stop**, so the cluster isn't kept busy (cost) |
# MAGIC | Bronze content | raw: timestamps stay text, duplicates kept | bronze is the faithful copy; silver cleans |
# MAGIC
# MAGIC Run it, then emit more days with notebook 11 and run it again: the second run loads only the new files.
# MAGIC
# MAGIC ⚠️ **Never delete the checkpoint on its own.** Auto Loader would forget what it loaded and load every file again.
# MAGIC To start over, drop `bronze.trade_events` **and** delete the checkpoint together.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

RUN_ID = str(uuid.uuid4())
LANDING = f"/Volumes/{CATALOG}/bronze/raw/trade_events"
CHECKPOINT = f"/Volumes/{CATALOG}/bronze/raw/_checkpoints/bronze_trade_events"
BRONZE = f"{CATALOG}.bronze.trade_events"

EVENT_SCHEMA = ("event_id STRING, trade_id BIGINT, account_id INT, ticker STRING, side STRING, "
                "quantity INT, price DOUBLE, fees DOUBLE, executed_at STRING, emitted_at STRING")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Define the stream and run it until there are no new files

# COMMAND ----------

events = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("rescuedDataColumn", "_rescued_data")          # fields not in the contract land here
    .schema(EVENT_SCHEMA)
    .load(LANDING)
    .select("*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.to_date(F.regexp_extract("_metadata.file_path", r"batch_date=(\d{4}-\d{2}-\d{2})", 1)).alias("_batch_date"),
            F.current_timestamp().alias("_ingested_at"),
            F.lit(RUN_ID).alias("_run_id")))

query = (events.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(BRONZE))
query.awaitTermination()

# Count what this run added straight from the table (every row carries this run's _run_id)
rows_this_run = spark.table(BRONZE).where(F.col("_run_id") == RUN_ID).count()
print(f"Loaded {rows_this_run:,} new rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Gate: landing → bronze
# MAGIC Compares bronze with what notebook 11 says it emitted (`audit.emit_manifest`), day by day.

# COMMAND ----------

def gate_row(check, sev, exp, act, status):
    return (RUN_ID, "landing_to_bronze", "bronze.trade_events", check, sev, str(exp), str(act), status,
            datetime.now(timezone.utc))

bronze = spark.table(BRONZE)
manifest = spark.table(f"{CATALOG}.audit.emit_manifest").groupBy("batch_date").agg(F.sum("events").alias("emitted"))
loaded = bronze.groupBy(F.col("_batch_date").alias("batch_date")).agg(F.count("*").alias("loaded"))

days = manifest.join(loaded, "batch_date", "full_outer").fillna(0, ["emitted", "loaded"])
missing_days = days.where("loaded < emitted").count()        # emitted but not (fully) loaded
extra_days = days.where("loaded > emitted").count()          # loaded more than emitted, e.g. a file loaded twice
files_twice = bronze.groupBy("_source_file").agg(F.countDistinct("_run_id").alias("runs")).where("runs > 1").count()
rescued = bronze.where("_rescued_data IS NOT NULL").count()
no_lineage = bronze.where("_source_file IS NULL OR _batch_date IS NULL OR _ingested_at IS NULL").count()
total_emitted, total_loaded = days.agg(F.sum("emitted"), F.sum("loaded")).first()

results = [
    gate_row("row_count_matches_emitted", "error", total_emitted, total_loaded,
             "PASS" if missing_days == 0 and extra_days == 0 else "FAIL"),
    gate_row("each_file_loaded_once", "error", 0, files_twice, "PASS" if files_twice == 0 else "FAIL"),
    gate_row("no_unexpected_fields", "warn", 0, rescued, "PASS" if rescued == 0 else "WARN"),
    gate_row("lineage_present", "error", 0, no_lineage, "PASS" if no_lineage == 0 else "FAIL"),
]

gate = spark.createDataFrame(results, spark.table(f"{CATALOG}.audit.gate_results").schema)
gate.write.mode("append").saveAsTable(f"{CATALOG}.audit.gate_results")
display(gate)
assert all(r[7] != "FAIL" for r in results), "❌ Gate landing_to_bronze FAILED. Silver must not run."
print(f"✅ bronze.trade_events: {total_loaded:,} rows · {days.count()} days · new this run {rows_this_run:,}")