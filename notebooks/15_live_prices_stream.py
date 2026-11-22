# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 15 · Live prices: consumer (ticks → bronze → 1-minute bars)
# MAGIC
# MAGIC Run this **at the same time** as notebook 14 (open it in a second tab). Two streams run side by side, each
# MAGIC waking up **every 10 seconds**:
# MAGIC
# MAGIC | Stream | From → to | What it does |
# MAGIC |---|---|---|
# MAGIC | A · bronze | tick files → `bronze.live_ticks` | Auto Loader, same pattern as the trade events |
# MAGIC | B · bars | `bronze.live_ticks` → `silver.live_bars_1m` | groups ticks into **1-minute bars** per symbol: open, high, low, close, volume, ticks, latency |
# MAGIC
# MAGIC **Watermark: 2 minutes.** A bar keeps accepting ticks until 2 minutes after it ends, then it's final. A tick that
# MAGIC arrives later than that is dropped and **counted** (the "late ticks" number for the README).
# MAGIC
# MAGIC Bars are written with `MERGE` in **update** mode, so the current minute's bar changes in front of you as ticks
# MAGIC arrive. That's what the screen recording shows.
# MAGIC
# MAGIC ⚠️ Streams with a time trigger keep the cluster busy. **Run the last cell (stop) when you're done.**

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

LANDING = f"/Volumes/{CATALOG}/bronze/raw/live_ticks"
CKPT = f"/Volumes/{CATALOG}/bronze/raw/_checkpoints"
BRONZE = f"{CATALOG}.bronze.live_ticks"
BARS = f"{CATALOG}.silver.live_bars_1m"
TRIGGER = "10 seconds"

TICK_SCHEMA = ("symbol STRING, price DOUBLE, volume DOUBLE, trade_ts_ms BIGINT, conditions ARRAY<STRING>, "
               "received_at STRING, session_id STRING")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BARS} (
    symbol            STRING,
    bar_start         TIMESTAMP COMMENT 'UTC minute the bar covers',
    bar_end           TIMESTAMP,
    open              DOUBLE,
    high              DOUBLE,
    low               DOUBLE,
    close             DOUBLE,
    volume            DOUBLE,
    ticks             BIGINT,
    avg_feed_latency_ms     DOUBLE COMMENT 'exchange time → received by the producer',
    avg_pipeline_latency_ms DOUBLE COMMENT 'exchange time → loaded into bronze',
    _updated_at       TIMESTAMP
)
CLUSTER BY (symbol, bar_start)
COMMENT 'Live 1-minute price bars from the Finnhub feed (streaming, updated every 10 seconds)'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## A · Tick files → bronze (Auto Loader)

# COMMAND ----------

ticks = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useIncrementalListing", "false")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(TICK_SCHEMA)
    .load(LANDING)
    .select("*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at")))

bronze_query = (ticks.writeStream
    .option("checkpointLocation", f"{CKPT}/bronze_live_ticks")
    .trigger(processingTime=TRIGGER)
    .queryName("live_ticks_to_bronze")
    .toTable(BRONZE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## B · Bronze → 1-minute bars (watermark + MERGE)
# MAGIC `min_by(price, event_time)` = the price of the **earliest** tick in the minute (the open);
# MAGIC `max_by(price, event_time)` = the price of the **latest** tick (the close).

# COMMAND ----------

import time
while not spark.catalog.tableExists(BRONZE):          # stream B needs the bronze table stream A creates
    time.sleep(5)

bars = (spark.readStream.table(BRONZE)
    .withColumn("event_time", F.timestamp_millis("trade_ts_ms"))
    .withColumn("received_ts", F.to_timestamp("received_at"))
    .withWatermark("event_time", "2 minutes")
    .groupBy("symbol", F.window("event_time", "1 minute").alias("w"))
    .agg(F.min_by("price", "event_time").alias("open"),
         F.max("price").alias("high"),
         F.min("price").alias("low"),
         F.max_by("price", "event_time").alias("close"),
         F.sum("volume").alias("volume"),
         F.count("*").alias("ticks"),
         F.avg(F.col("received_ts").cast("double") * 1000 - F.col("trade_ts_ms")).alias("avg_feed_latency_ms"),
         F.avg(F.col("_ingested_at").cast("double") * 1000 - F.col("trade_ts_ms")).alias("avg_pipeline_latency_ms"))
    .select("symbol", F.col("w.start").alias("bar_start"), F.col("w.end").alias("bar_end"),
            "open", "high", "low", "close", "volume", "ticks", "avg_feed_latency_ms", "avg_pipeline_latency_ms"))


def merge_bars(batch, batch_id):
    """Each micro-batch holds the bars that changed: insert new minutes, overwrite updated ones."""
    (DeltaTable.forName(spark, BARS).alias("t")
        .merge(batch.withColumn("_updated_at", F.current_timestamp()).alias("s"),
               "t.symbol = s.symbol AND t.bar_start = s.bar_start")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())


bars_query = (bars.writeStream
    .outputMode("update")                              # emit a bar every time it changes, not only when final
    .foreachBatch(merge_bars)
    .option("checkpointLocation", f"{CKPT}/silver_live_bars_1m")
    .trigger(processingTime=TRIGGER)
    .queryName("live_bars_1m")
    .start())

print("▶️ Both streams running. Start notebook 14 in another tab if it isn't already.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Watch it live
# MAGIC Re-run this cell (or use the cell's auto-refresh) to see the current minute's bars change.

# COMMAND ----------

display(spark.sql(f"""
    SELECT symbol, date_format(bar_start, 'HH:mm') AS minute_utc, open, high, low, close, ticks,
           round(avg_feed_latency_ms) AS feed_ms, round(avg_pipeline_latency_ms / 1000, 1) AS pipeline_s
    FROM {BARS}
    WHERE bar_start >= current_timestamp() - INTERVAL 10 MINUTES
    ORDER BY bar_start DESC, ticks DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Session numbers (after the producer stops)
# MAGIC Reconciliation: every tick the producer logged in `audit.live_sessions` must be in bronze.

# COMMAND ----------

progress = bars_query.lastProgress or {}
dropped = sum(op.get("numRowsDroppedByWatermark", 0) for op in progress.get("stateOperators", []))

display(spark.sql(f"""
    SELECT s.session_id, s.started_at, round(s.minutes, 1) AS minutes, s.ticks AS ticks_produced,
           b.ticks_in_bronze, s.ticks = b.ticks_in_bronze AS reconciled,
           round(s.ticks / s.minutes) AS ticks_per_min, s.reconnects
    FROM {CATALOG}.audit.live_sessions s
    LEFT JOIN (SELECT session_id, count(*) AS ticks_in_bronze FROM {BRONZE} GROUP BY session_id) b
      USING (session_id)
    ORDER BY s.started_at DESC
"""))
print(f"Late ticks dropped by the watermark (last micro-batch): {dropped}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⏹ Stop both streams (always run this when done)

# COMMAND ----------

for q in spark.streams.active:
    print("stopping", q.name)
    q.stop()

# COMMAND ----------

from pyspark.sql import functions as F
CATALOG = 'brokerage_dev'  
landed = (spark.read.text(f"/Volumes/{CATALOG}/bronze/raw/live_ticks/*/*.json")
          .select(F.col("_metadata.file_path").alias("f")).distinct()
          .withColumn("session", F.regexp_extract("f", r"_([0-9a-f]{8})\.json$", 1))
          .groupBy("session").agg(F.count("*").alias("files_landed")))
loaded = (spark.table(f"{CATALOG}.bronze.live_ticks")
          .groupBy(F.substring("session_id", 1, 8).alias("session"))
          .agg(F.countDistinct("_source_file").alias("files_in_bronze")))
display(landed.join(loaded, "session", "left"))

# COMMAND ----------

from pyspark.sql import functions as F
clean = lambda c: F.regexp_replace(c, "^dbfs:", "")
on_disk = (spark.read.text(f"/Volumes/{CATALOG}/bronze/raw/live_ticks/*/*.json")
           .groupBy(clean(F.col("_metadata.file_path")).alias("file")).agg(F.count("*").alias("rows_on_disk")))
in_bronze = (spark.table(f"{CATALOG}.bronze.live_ticks")
             .groupBy(clean(F.col("_source_file")).alias("file")).agg(F.count("*").alias("rows_in_bronze")))
display(on_disk.join(in_bronze, "file", "left").where("rows_in_bronze IS NULL OR rows_on_disk != rows_in_bronze"))

# COMMAND ----------

print(" ".join(dbutils.secrets.get("brokerage-dev", "mssql_password")))

# COMMAND ----------

