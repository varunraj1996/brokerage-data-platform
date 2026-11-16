# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 06 · Silver: clean and validate daily prices
# MAGIC
# MAGIC Turns the raw Tiingo download (`bronze.tiingo_prices`) into **`silver.daily_prices`**: one trusted row per
# MAGIC **ticker × trading day**.
# MAGIC
# MAGIC | Step | What happens |
# MAGIC |---|---|
# MAGIC | 1 · Type | text date → real date; keep only the columns we use |
# MAGIC | 2 · Deduplicate | one row per ticker per day |
# MAGIC | 3 · Validate | prices positive, `low ≤ open/close ≤ high`, sane split factor and dividend, ticker in the universe; failures → **quarantine** |
# MAGIC | 4 · Split-adjust | `split_adjusted_close` = close ÷ every later split, so a share bought before NVIDIA's 10-for-1 split is valued correctly after it |
# MAGIC | 5 · Gate | rows in = out + duplicates + rejects; key unique; every ticker present; reject-rate limit |
# MAGIC | 6 · Publish | `MERGE` into silver, only if the gate passed |

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import uuid
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

RUN_ID = str(uuid.uuid4())
MAX_REJECT_RATE = 0.01
SILVER = f"{CATALOG}.silver.daily_prices"
KEY = ["ticker", "trade_date"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1–2 · Latest bronze copy, typed and deduplicated

# COMMAND ----------

bronze = spark.table(f"{CATALOG}.bronze.tiingo_prices")
last_run = bronze.agg(F.max_by("_run_id", "_ingested_at")).first()[0]

df = (bronze.filter(F.col("_run_id") == last_run)
      .select(F.upper(F.trim("ticker")).alias("ticker"),
              F.to_date(F.substring("date", 1, 10)).alias("trade_date"),
              "open", "high", "low", "close", "volume",
              F.col("adjClose").alias("adj_close"),
              F.col("divCash").alias("dividend_cash"),
              F.col("splitFactor").alias("split_factor"),
              "_ingested_at"))
rows_in = df.count()

w = Window.partitionBy(*KEY).orderBy(F.col("_ingested_at").desc())
ranked = df.withColumn("_rn", F.row_number().over(w))
duplicates = ranked.filter("_rn > 1").count()
df = ranked.filter("_rn = 1").drop("_rn")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Validate
# MAGIC A tiny tolerance (`1e-6`) allows for rounding in the source.

# COMMAND ----------

RULES = {
    "date_present":           "trade_date IS NOT NULL",
    "prices_positive":        "open > 0 AND high > 0 AND low > 0 AND close > 0",
    "high_not_below_low":     "high >= low",
    "open_within_day_range":  "open BETWEEN low - 1e-6 AND high + 1e-6",
    "close_within_day_range": "close BETWEEN low - 1e-6 AND high + 1e-6",
    "volume_not_negative":    "volume >= 0",
    "split_factor_positive":  "split_factor > 0",
    "dividend_not_negative":  "dividend_cash >= 0",
    "date_in_window":         "trade_date BETWEEN DATE'2024-01-01' AND DATE'2025-12-31'",
}

universe = spark.table(f"{CATALOG}.gold.ref_universe").select(F.col("ticker").alias("_u"))
df = df.join(universe, df.ticker == universe._u, "left")

failed = [F.when(~F.expr(c) | F.expr(c).isNull(), F.lit(r)) for r, c in RULES.items()]
failed.append(F.when(F.col("_u").isNull(), F.lit("ticker_not_in_universe")))
df = df.withColumn("_reject_reason", F.concat_ws("; ", *failed)).drop("_u")

valid = df.filter("_reject_reason = ''").drop("_reject_reason")
rejects = df.filter("_reject_reason != ''")
rows_valid, rows_rejected = valid.count(), rejects.count()

if rows_rejected:
    record_cols = [c for c in rejects.columns if c != "_reject_reason"]
    (rejects.select(F.lit(RUN_ID).alias("run_id"), F.lit("daily_prices").alias("table_name"),
                    F.col("_reject_reason").alias("reject_reason"),
                    F.to_json(F.struct(*record_cols)).alias("record"),
                    F.current_timestamp().alias("quarantined_at"))
     .write.mode("append").saveAsTable(f"{CATALOG}.audit.quarantine"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Split-adjusted close
# MAGIC `split_factor` is 10 on the day of a 10-for-1 split and 1 on every other day. For each day we multiply all
# MAGIC **later** split factors together and divide the close by that, so every price is in today's share units.
# MAGIC Example: NVIDIA closed near $1,200 before its June 2024 split → split-adjusted ≈ $120.

# COMMAND ----------

later = Window.partitionBy("ticker").orderBy("trade_date").rowsBetween(1, Window.unboundedFollowing)
valid = (valid
         .withColumn("later_splits", F.coalesce(F.exp(F.sum(F.log("split_factor")).over(later)), F.lit(1.0)))
         .withColumn("split_adjusted_close", F.round(F.col("close") / F.col("later_splits"), 6))
         .drop("later_splits", "_ingested_at")
         .withColumn("_bronze_run_id", F.lit(last_run))
         .withColumn("_silver_loaded_at", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Gate

# COMMAND ----------

def gate_row(check, sev, exp, act, status):
    return (RUN_ID, "bronze_to_silver", "silver.daily_prices", check, sev, str(exp), str(act), status,
            datetime.now(timezone.utc))

expected_tickers = universe.count()
tickers_valid = valid.select("ticker").distinct().count()
keys_unique = valid.select(*KEY).distinct().count() == rows_valid
reject_rate = rows_rejected / rows_in if rows_in else 0

results = [
    gate_row("rows_in_equals_out_plus_dupes_plus_rejects", "error", rows_in,
             rows_valid + duplicates + rows_rejected,
             "PASS" if rows_in == rows_valid + duplicates + rows_rejected else "FAIL"),
    gate_row("ticker_day_unique", "error", rows_valid, "unique" if keys_unique else "duplicates",
             "PASS" if keys_unique else "FAIL"),
    gate_row("every_ticker_present", "error", expected_tickers, tickers_valid,
             "PASS" if tickers_valid == expected_tickers else "FAIL"),
    gate_row("reject_rate", "warn", f"<= {MAX_REJECT_RATE:.0%}", f"{reject_rate:.2%}",
             "PASS" if rows_rejected == 0 else ("WARN" if reject_rate <= MAX_REJECT_RATE else "FAIL")),
]
passed = all(r[7] != "FAIL" for r in results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Publish (only if the gate passed) and record the gate

# COMMAND ----------

if passed:
    if not spark.catalog.tableExists(SILVER):
        valid.limit(0).write.saveAsTable(SILVER)
    (DeltaTable.forName(spark, SILVER).alias("s")
        .merge(valid.alias("n"), "s.ticker = n.ticker AND s.trade_date = n.trade_date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute())
    silver_rows = spark.table(SILVER).count()
    results.append(gate_row("silver_rows_equal_valid_rows", "error", rows_valid, silver_rows,
                            "PASS" if silver_rows == rows_valid else "FAIL"))

gate = spark.createDataFrame(results, spark.table(f"{CATALOG}.audit.gate_results").schema)
gate.write.mode("append").saveAsTable(f"{CATALOG}.audit.gate_results")
display(gate)

assert all(r[7] != "FAIL" for r in results), "❌ Gate bronze_to_silver (prices) FAILED. dbt must not run."
print(f"✅ silver.daily_prices: in {rows_in:,} · valid {rows_valid:,} · duplicates {duplicates:,} · "
      f"rejected {rows_rejected:,} · tickers {tickers_valid}")