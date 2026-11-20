# Databricks notebook source
# MAGIC %md
# MAGIC # 11 · Emit trade events (the simulated stream)
# MAGIC
# MAGIC In a real brokerage, every execution is sent as a small **message** the moment it happens. This notebook
# MAGIC replays the truth in `sim.trades` as JSON message files in the Volume, **one folder per day**, the way a
# MAGIC message system would deliver them. It adds the mess real streams have:
# MAGIC
# MAGIC | Mess | How much | Why it matters |
# MAGIC |---|---|---|
# MAGIC | **Duplicates** | ~0.5% of trades sent twice (same `event_id`) | the stream must count each trade once |
# MAGIC | **Late events** | ~0.3% arrive 1–5 days after the trade | they land in a later day's folder; the trade's date must not change |
# MAGIC | **Out of order** | rows inside a file are shuffled | order can't be trusted |
# MAGIC
# MAGIC Each run emits the days **after the last emitted day, up to `emit_until`**, so you can run it in steps and
# MAGIC watch the stream pick up only the new files.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

dbutils.widgets.text("emit_until", "2025-06-30", "Emit days up to (yyyy-mm-dd)")
EMIT_UNTIL = dbutils.widgets.get("emit_until")

RUN_ID = str(uuid.uuid4())
SEED = 7
DUPLICATE_RATE, LATE_RATE = 0.005, 0.003
LANDING = f"/Volumes/{CATALOG}/bronze/raw/trade_events"
MANIFEST = f"{CATALOG}.audit.emit_manifest"
ISO = "yyyy-MM-dd'T'HH:mm:ss'Z'"

spark.sql(f"""CREATE TABLE IF NOT EXISTS {MANIFEST}
              (run_id STRING, batch_date DATE, events BIGINT, duplicates BIGINT, late BIGINT, emitted_at TIMESTAMP)""")
last_done = spark.table(MANIFEST).agg(F.max("batch_date")).first()[0]
print(f"Already emitted up to: {last_done or 'nothing yet'} · this run: up to {EMIT_UNTIL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Turn each trade into an event, then add duplicates and late arrivals
# MAGIC A hash of `trade_id` gives every trade a repeatable random number, so the same trades are duplicated or late on
# MAGIC every run. `batch_date` is the day the message **arrives**, which decides its folder.

# COMMAND ----------

trades = spark.table(f"{CATALOG}.sim.trades")

def rand01(salt):
    """A repeatable random number in [0, 1) per trade: the same trade gets the same number on every run."""
    return (F.abs(F.xxhash64("trade_id", F.lit(salt))) % 1_000_000) / 1_000_000


events = (trades
    .withColumn("event_id", F.concat(F.lit("te-"), F.sha2(F.col("trade_id").cast("string"), 256).substr(1, 24)))
    .withColumn("r", rand01(SEED))
    .withColumn("is_late", F.col("r") < LATE_RATE)
    # normal: sent 1–5 seconds after execution · late: 1–5 days later
    .withColumn("delay_s", F.when(F.col("is_late"), (1 + F.floor(rand01(SEED + 1) * 5)) * 86400)
                            .otherwise(1 + F.floor(rand01(SEED + 2) * 5)))
    .withColumn("emitted_ts", (F.col("executed_at").cast("long") + F.col("delay_s")).cast("timestamp")))

dupes = (events.where((F.col("r") >= LATE_RATE) & (F.col("r") < LATE_RATE + DUPLICATE_RATE))
         .withColumn("emitted_ts", (F.col("emitted_ts").cast("long") + 30).cast("timestamp"))   # resent 30 s later
         .withColumn("is_late", F.lit(False)))

stream = (events.unionByName(dupes)
    .withColumn("batch_date", F.to_date("emitted_ts"))
    .select("event_id", "trade_id", "account_id", "ticker", "side", "quantity", "price", "fees",
            F.date_format("executed_at", ISO).alias("executed_at"),
            F.date_format("emitted_ts", ISO).alias("emitted_at"),
            "batch_date", "is_late"))

window = stream.where(F.col("batch_date") <= F.lit(EMIT_UNTIL))
if last_done:
    window = window.where(F.col("batch_date") > F.lit(last_done))
window = window.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Write one folder per arrival day, shuffled, and check every event landed

# COMMAND ----------

summary = (window.groupBy("batch_date").agg(
               F.count("*").alias("events"),
               (F.count("*") - F.countDistinct("event_id")).alias("duplicates"),
               F.sum(F.col("is_late").cast("int")).alias("late"))
           .orderBy("batch_date").collect())

if not summary:
    print("Nothing new to emit. Raise emit_until to send more days.")
else:
    (window.drop("is_late")
        .repartition("batch_date")                     # one file per day
        .sortWithinPartitions(F.rand(SEED + 3))        # out of order inside each file
        .write.mode("append").partitionBy("batch_date").json(LANDING))

    # Gate (source → landing): what we read back from the files = what we meant to write
    expected = sum(r.events for r in summary)
    days = [r.batch_date for r in summary]
    landed = (spark.read.json(LANDING)
              .where(F.col("batch_date").between(min(days), max(days))).count())
    assert landed == expected, f"❌ Landing check failed: wrote {expected:,} events, read back {landed:,}"

    now = datetime.now(timezone.utc)
    (spark.createDataFrame([(RUN_ID, r.batch_date, r.events, r.duplicates, r.late, now) for r in summary],
                           spark.table(MANIFEST).schema)
     .write.mode("append").saveAsTable(MANIFEST))
    print(f"✅ Emitted {expected:,} events over {len(days)} days ({days[0]} → {days[-1]}) · "
          f"duplicates {sum(r.duplicates for r in summary):,} · late {sum(r.late for r in summary):,} · "
          f"read back {landed:,}")

# COMMAND ----------

display(spark.table(MANIFEST).orderBy(F.desc("batch_date")).limit(10))

# COMMAND ----------

