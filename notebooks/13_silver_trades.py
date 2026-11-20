# Databricks notebook source
# MAGIC %md
# MAGIC # 13 · Silver: trades (streaming dedupe + MERGE)
# MAGIC
# MAGIC Reads **new rows** from `bronze.trade_events` as a stream and builds **`silver.trades`**: one trusted row per trade.
# MAGIC
# MAGIC | Step | What happens | Why |
# MAGIC |---|---|---|
# MAGIC | 1 · Parse | text timestamps → real UTC timestamps; `trade_date` = the **New York** date of the execution | a trading day is a New York day |
# MAGIC | 2 · Watermark + dedupe | `dropDuplicatesWithinWatermark(event_id)`, watermark **7 days** on `executed_at` | a message sent twice counts once; Spark only remembers ids for 7 days, so its memory stays small |
# MAGIC | 3 · Validate | side, quantity, price, fees, account open on that day, stock known, price inside the day's real low–high | failures → `audit.quarantine` with every reason |
# MAGIC | 4 · `MERGE` on `trade_id` | insert only trades not already in silver | re-running, or a duplicate older than 7 days, can never create a second row |
# MAGIC | 5 · Gate | every distinct trade in bronze is in silver or quarantine; `trade_id` unique | proves nothing was lost or doubled |
# MAGIC
# MAGIC **Late events are kept.** A trade that arrives 5 days late still gets its real `trade_date` (from `executed_at`,
# MAGIC not from when it arrived). The watermark only limits how long Spark watches for *duplicates*.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import uuid
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

RUN_ID = str(uuid.uuid4())
BRONZE = f"{CATALOG}.bronze.trade_events"
SILVER = f"{CATALOG}.silver.trades"
CHECKPOINT = f"/Volumes/{CATALOG}/bronze/raw/_checkpoints/silver_trades"
MAX_REJECT_RATE = 0.01

# The table is created once, with liquid clustering on the columns most queries filter by
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER} (
    trade_id          BIGINT    COMMENT 'business key: one row per execution',
    event_id          STRING    COMMENT 'id of the message that delivered it',
    account_id        INT,
    ticker            STRING,
    side              STRING    COMMENT 'BUY or SELL',
    quantity          INT       COMMENT 'shares, split-adjusted units, always positive',
    price             DOUBLE    COMMENT 'execution price per share (split-adjusted)',
    fees              DOUBLE,
    gross_amount      DOUBLE    COMMENT 'quantity × price',
    cash_impact       DOUBLE    COMMENT 'BUY: −(gross + fees) · SELL: gross − fees',
    executed_at       TIMESTAMP COMMENT 'UTC',
    trade_date        DATE      COMMENT 'New York date of the execution',
    emitted_at        TIMESTAMP COMMENT 'UTC, when the message was sent',
    arrival_delay_s   BIGINT    COMMENT 'emitted_at − executed_at in seconds',
    is_late           BOOLEAN   COMMENT 'arrived on a later day than it was executed',
    _source_file      STRING,
    _bronze_run_id    STRING,
    _silver_run_id    STRING,
    _silver_loaded_at TIMESTAMP
)
CLUSTER BY (trade_date, account_id)
COMMENT 'One row per trade execution, deduplicated and validated'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1–2 · Read new bronze rows, parse, and drop duplicate messages

# COMMAND ----------

parsed = (spark.readStream.table(BRONZE)
    .select("event_id", "trade_id", "account_id", F.upper(F.trim("ticker")).alias("ticker"),
            F.upper(F.trim("side")).alias("side"), "quantity", "price", "fees",
            F.to_timestamp("executed_at", "yyyy-MM-dd'T'HH:mm:ssX").alias("executed_at"),
            F.to_timestamp("emitted_at", "yyyy-MM-dd'T'HH:mm:ssX").alias("emitted_at"),
            "_source_file", F.col("_run_id").alias("_bronze_run_id"))
    .withColumn("trade_date", F.to_date(F.from_utc_timestamp("executed_at", "America/New_York")))
    .withColumn("arrival_delay_s", F.col("emitted_at").cast("long") - F.col("executed_at").cast("long"))
    .withColumn("is_late", F.to_date(F.from_utc_timestamp("emitted_at", "America/New_York")) > F.col("trade_date")))

deduped = (parsed
    .withWatermark("executed_at", "7 days")
    .dropDuplicatesWithinWatermark(["event_id"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3–4 · For each micro-batch: validate, quarantine, MERGE
# MAGIC `foreachBatch` hands us each micro-batch as a normal DataFrame, so we can join reference tables and run a `MERGE`.

# COMMAND ----------

RULES = {
    "ids_present":           "trade_id IS NOT NULL AND event_id IS NOT NULL AND account_id IS NOT NULL",
    "side_valid":            "side IN ('BUY', 'SELL')",
    "quantity_positive":     "quantity > 0",
    "price_positive":        "price > 0",
    "fees_not_negative":     "fees >= 0",
    "executed_at_valid":     "executed_at IS NOT NULL AND executed_at <= current_timestamp()",
    "account_exists":        "_acct IS NOT NULL",
    "account_open_that_day": "_acct IS NULL OR trade_date BETWEEN _opened AND coalesce(_closed, DATE'9999-12-31')",
    "price_known_that_day":  "_low IS NOT NULL",
    "price_within_day_range": "_low IS NULL OR price BETWEEN _low - 0.005 AND _high + 0.005",
}

batch_stats = []


def process_batch(batch, batch_id):
    accounts = spark.table(f"{CATALOG}.silver.accounts").select(
        F.col("account_id").alias("_acct"), F.col("opened_date").alias("_opened"), F.col("closed_date").alias("_closed"))
    prices = (spark.table(f"{CATALOG}.silver.daily_prices")
        .withColumn("_f", F.col("split_adjusted_close") / F.col("close"))
        .select(F.col("ticker").alias("_tk"), F.col("trade_date").alias("_d"),
                (F.col("low") * F.col("_f")).alias("_low"), (F.col("high") * F.col("_f")).alias("_high")))

    # Safety net: one row per trade inside this batch (MERGE needs a unique source key)
    w = Window.partitionBy("trade_id").orderBy("emitted_at", "event_id")
    batch = batch.withColumn("_rn", F.row_number().over(w))
    in_batch_dupes = batch.where("_rn > 1").count()
    batch = batch.where("_rn = 1").drop("_rn")

    checked = (batch
        .join(F.broadcast(accounts), batch.account_id == accounts._acct, "left")
        .join(prices, (batch.ticker == prices._tk) & (batch.trade_date == prices._d), "left"))
    failed = [F.when(~F.expr(c) | F.expr(c).isNull(), F.lit(r)) for r, c in RULES.items()]
    checked = checked.withColumn("_reject_reason", F.concat_ws("; ", *failed)).cache()

    helper = ["_acct", "_opened", "_closed", "_tk", "_d", "_low", "_high"]
    rejects = checked.where("_reject_reason != ''")
    valid = (checked.where("_reject_reason = ''").drop("_reject_reason", *helper)
        .withColumn("gross_amount", F.round(F.col("quantity") * F.col("price"), 2))
        .withColumn("cash_impact", F.when(F.col("side") == "BUY", -(F.col("gross_amount") + F.col("fees")))
                                    .otherwise(F.col("gross_amount") - F.col("fees")))
        .withColumn("_silver_run_id", F.lit(RUN_ID))
        .withColumn("_silver_loaded_at", F.current_timestamp())
        .select(*spark.table(SILVER).columns))

    n_in, n_rejects = checked.count(), rejects.count()
    if n_rejects:
        record_cols = [c for c in checked.columns if c not in helper + ["_reject_reason"]]
        (rejects.select(F.lit(RUN_ID).alias("run_id"), F.lit("trades").alias("table_name"),
                        F.col("_reject_reason").alias("reject_reason"),
                        F.to_json(F.struct(*record_cols)).alias("record"),
                        F.current_timestamp().alias("quarantined_at"))
         .write.mode("append").saveAsTable(f"{CATALOG}.audit.quarantine"))

    before = spark.table(SILVER).count()
    (DeltaTable.forName(spark, SILVER).alias("s")
        .merge(valid.alias("n"), "s.trade_id = n.trade_id")
        .whenNotMatchedInsertAll()
        .execute())
    inserted = spark.table(SILVER).count() - before

    batch_stats.append({"batch_id": batch_id, "rows_in": n_in + in_batch_dupes, "in_batch_dupes": in_batch_dupes,
                        "rejected": n_rejects, "inserted": inserted,
                        "already_in_silver": n_in - n_rejects - inserted})
    checked.unpersist()

# COMMAND ----------

query = (deduped.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start())
query.awaitTermination()

for s in batch_stats:
    print(s)
print(f"Inserted {sum(s['inserted'] for s in batch_stats):,} new trades in {len(batch_stats)} micro-batch(es)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Gate: bronze → silver (on the **whole** tables, not just this run)

# COMMAND ----------

def gate_row(check, sev, exp, act, status):
    return (RUN_ID, "bronze_to_silver", "silver.trades", check, sev, str(exp), str(act), status,
            datetime.now(timezone.utc))

bronze = spark.table(BRONZE)
silver = spark.table(SILVER)
quarantined = (spark.table(f"{CATALOG}.audit.quarantine").where("table_name = 'trades'")
               .select(F.get_json_object("record", "$.trade_id").cast("bigint").alias("trade_id")).distinct())

bronze_rows = bronze.count()
bronze_trades = bronze.select("trade_id").distinct()
n_bronze_trades = bronze_trades.count()
silver_rows = silver.count()
n_silver_ids = silver.select("trade_id").distinct().count()
rejected_only = quarantined.join(silver.select("trade_id"), "trade_id", "left_anti").count()
lost = bronze_trades.join(silver.select("trade_id"), "trade_id", "left_anti") \
                    .join(quarantined, "trade_id", "left_anti").count()
duplicates_removed = bronze_rows - n_bronze_trades
reject_rate = rejected_only / n_bronze_trades if n_bronze_trades else 0

results = [
    gate_row("every_bronze_trade_in_silver_or_quarantine", "error", 0, lost, "PASS" if lost == 0 else "FAIL"),
    gate_row("trades_accounted_for", "error", n_bronze_trades, f"{silver_rows} silver + {rejected_only} quarantined",
             "PASS" if n_bronze_trades == silver_rows + rejected_only else "FAIL"),
    gate_row("trade_id_unique", "error", silver_rows, n_silver_ids, "PASS" if silver_rows == n_silver_ids else "FAIL"),
    gate_row("reject_rate", "warn", f"<= {MAX_REJECT_RATE:.0%}", f"{reject_rate:.2%}",
             "PASS" if rejected_only == 0 else ("WARN" if reject_rate <= MAX_REJECT_RATE else "FAIL")),
    gate_row("duplicate_messages_removed", "warn", "logged", duplicates_removed, "PASS"),
]

gate = spark.createDataFrame(results, spark.table(f"{CATALOG}.audit.gate_results").schema)
gate.write.mode("append").saveAsTable(f"{CATALOG}.audit.gate_results")
display(gate)
assert all(r[7] != "FAIL" for r in results), "❌ Gate bronze_to_silver (trades) FAILED. dbt must not run."
print(f"✅ silver.trades: {silver_rows:,} trades · {duplicates_removed:,} duplicate messages removed · "
      f"{rejected_only:,} quarantined · {silver.where('is_late').count():,} late arrivals kept")