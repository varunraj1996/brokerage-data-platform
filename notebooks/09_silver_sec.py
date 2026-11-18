# Databricks notebook source
# MAGIC %md
# MAGIC # 09 · Silver: securities and company name history (SEC)
# MAGIC
# MAGIC Builds two clean tables from the latest SEC snapshot in bronze:
# MAGIC
# MAGIC | Table | One row per | What's cleaned |
# MAGIC |---|---|---|
# MAGIC | `silver.securities` | ticker in the universe | CIK from the SEC's ticker list; exchange matched to **our** ticker (not a company's preferred shares); legal name kept as filed, plus a version without the state suffix (`/DE/`, `/NEW`); display name and sector from `ref_universe` |
# MAGIC | `silver.security_name_history` | company × name period | former names + current name; periods **inside** another name's period are resolved (e.g. Apple's one-day `APPLE COMPUTER INC/ FA` in 1997); periods made contiguous; first period back-dated to 1900-01-01 so every date finds a name |
# MAGIC
# MAGIC **Gates:** every universe ticker has exactly one security; every company has exactly one current name;
# MAGIC name periods have no overlaps and no gaps. Removed overlapping periods are logged to `audit.quarantine`.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import uuid
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

RUN_ID = str(uuid.uuid4())
OPEN_END = "9999-12-31"
FIRST_START = "1900-01-01"


def latest(table):
    df = spark.table(f"{CATALOG}.bronze.{table}")
    last = df.agg(F.max_by("_run_id", "_ingested_at")).first()[0]
    return df.filter(F.col("_run_id") == last)


def gate_row(table, check, sev, exp, act, status):
    return (RUN_ID, "bronze_to_silver", table, check, sev, str(exp), str(act), status,
            datetime.now(timezone.utc))


universe = spark.table(f"{CATALOG}.gold.ref_universe").select(
    "ticker", F.col("company_name").alias("display_name"), "sector", "is_benchmark")
submissions = latest("sec_submissions")
sec_tickers = latest("sec_company_tickers")
snapshot = submissions.select("_snapshot").first()[0]
results = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · `silver.securities`

# COMMAND ----------

ticker_to_cik = (sec_tickers.select(F.upper(F.trim("ticker")).alias("ticker"), "cik")
                 .dropDuplicates(["ticker"]))

pos = F.array_position(F.col("tickers"), F.col("ticker"))          # 1-based; 0 if our ticker isn't listed

securities = (universe
    .join(ticker_to_cik, "ticker", "left")
    .join(submissions.select("cik", F.col("name").alias("legal_name"), F.col("sic").alias("sic_code"),
                             F.col("sicDescription").alias("sic_description"),
                             F.col("entityType").alias("entity_type"), "tickers", "exchanges"),
          "cik", "left")
    .withColumn("exchange", F.when(pos > 0, F.element_at("exchanges", pos.cast("int"))))
    .withColumn("legal_name_clean", F.trim(F.regexp_replace("legal_name", r"\s*/[A-Za-z]+/?\s*$", "")))
    .select("ticker", "cik", "display_name", "legal_name", "legal_name_clean", "exchange",
            "sic_code", "sic_description", "entity_type", "sector", "is_benchmark")
    .withColumn("_snapshot", F.lit(snapshot))
    .withColumn("_silver_loaded_at", F.current_timestamp()))

n_universe = universe.count()
n_rows = securities.count()
n_tickers = securities.select("ticker").distinct().count()
no_cik = [r.ticker for r in securities.where("cik IS NULL OR legal_name IS NULL").collect()]
no_exchange = [r.ticker for r in securities.where("cik IS NOT NULL AND exchange IS NULL").collect()]

results += [
    gate_row("silver.securities", "every_universe_ticker_once", "error", n_universe, f"{n_rows} rows / {n_tickers} tickers",
             "PASS" if n_rows == n_tickers == n_universe else "FAIL"),
    gate_row("silver.securities", "cik_and_legal_name_found", "error", 0, no_cik or 0,
             "PASS" if not no_cik else "FAIL"),
    gate_row("silver.securities", "exchange_matched_to_our_ticker", "warn", 0, no_exchange or 0,
             "PASS" if not no_exchange else "WARN"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · `silver.security_name_history`
# MAGIC 1. Former names (dates as filed) + the current name (starting the day after the last former name ended).
# MAGIC 2. A period that sits **entirely inside** another name's period is removed and logged; it can't be "the" name
# MAGIC    on those days if another name already covers them.
# MAGIC 3. Periods are made **contiguous**: each name lasts until the day before the next one starts, the first starts
# MAGIC    on 1900-01-01 and the current one ends 9999-12-31. Every date maps to exactly one name.

# COMMAND ----------

former = (submissions
    .select("cik", F.explode_outer("formerNames").alias("f"))
    .where(F.col("f").isNotNull())
    .select("cik", F.col("f.name").alias("name"),
            F.to_date(F.substring("f.from", 1, 10)).alias("start_date"),
            F.to_date(F.substring("f.to", 1, 10)).alias("end_date")))

last_former_end = former.groupBy("cik").agg(F.max("end_date").alias("last_end"))
current = (submissions.select("cik", F.col("name"))
    .join(last_former_end, "cik", "left")
    .select("cik", "name",
            F.coalesce(F.date_add("last_end", 1), F.lit(FIRST_START).cast("date")).alias("start_date"),
            F.lit(OPEN_END).cast("date").alias("end_date")))

periods = former.unionByName(current).withColumn("pid", F.monotonically_increasing_id())

# Periods inside another period of the same company (identical periods: keep the one with the lower id)
a, b = periods.alias("a"), periods.alias("b")
inside = (a.join(b, (F.col("a.cik") == F.col("b.cik")) & (F.col("a.pid") != F.col("b.pid")) &
                    (F.col("a.start_date") >= F.col("b.start_date")) & (F.col("a.end_date") <= F.col("b.end_date")) &
                    ~((F.col("a.start_date") == F.col("b.start_date")) & (F.col("a.end_date") == F.col("b.end_date")) &
                      (F.col("a.pid") < F.col("b.pid"))))
           .select("a.*").dropDuplicates(["pid"]))
bad_dates = periods.where("start_date IS NULL OR end_date IS NULL OR start_date > end_date")
removed = inside.unionByName(bad_dates).dropDuplicates(["pid"])

kept = periods.join(removed.select("pid"), "pid", "left_anti")
w = Window.partitionBy("cik").orderBy("start_date", "pid")
history = (kept
    .withColumn("rn", F.row_number().over(w))
    .withColumn("next_start", F.lead("start_date").over(w))
    .select("cik", F.col("name").alias("legal_name"),
            F.when(F.col("rn") == 1, F.lit(FIRST_START).cast("date")).otherwise(F.col("start_date")).alias("valid_from"),
            F.coalesce(F.date_sub("next_start", 1), F.lit(OPEN_END).cast("date")).alias("valid_to"))
    .withColumn("is_current", F.col("valid_to") == F.lit(OPEN_END).cast("date"))
    .withColumn("_snapshot", F.lit(snapshot))
    .withColumn("_silver_loaded_at", F.current_timestamp()))

# Log removed periods (kept, never silently dropped)
n_removed = removed.count()
if n_removed:
    (removed.select(F.lit(RUN_ID).alias("run_id"), F.lit("security_name_history").alias("table_name"),
                    F.when(F.col("start_date").isNull() | F.col("end_date").isNull() |
                           (F.col("start_date") > F.col("end_date")), "invalid_period")
                     .otherwise("period_inside_another_name").alias("reject_reason"),
                    F.to_json(F.struct("cik", "name", "start_date", "end_date")).alias("record"),
                    F.current_timestamp().alias("quarantined_at"))
     .write.mode("append").saveAsTable(f"{CATALOG}.audit.quarantine"))

# Gates on the history
n_companies = submissions.select("cik").distinct().count()
n_current = history.where("is_current").count()
hw = Window.partitionBy("cik").orderBy("valid_from")
breaks = (history.withColumn("next_from", F.lead("valid_from").over(hw))
          .where("next_from IS NOT NULL AND next_from != date_add(valid_to, 1)").count())

results += [
    gate_row("silver.security_name_history", "one_current_name_per_company", "error",
             n_companies, n_current, "PASS" if n_current == n_companies else "FAIL"),
    gate_row("silver.security_name_history", "no_overlaps_or_gaps", "error", 0, breaks,
             "PASS" if breaks == 0 else "FAIL"),
    gate_row("silver.security_name_history", "overlapping_periods_resolved", "warn", "logged",
             n_removed, "PASS"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Publish (only if the gates passed) and record them

# COMMAND ----------

passed = all(r[7] != "FAIL" for r in results)
if passed:
    target = f"{CATALOG}.silver.securities"
    if not spark.catalog.tableExists(target):
        securities.limit(0).write.saveAsTable(target)
    (DeltaTable.forName(spark, target).alias("s")
        .merge(securities.alias("n"), "s.ticker = n.ticker")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().whenNotMatchedBySourceDelete().execute())

    # The full name history is rebuilt from the latest SEC snapshot every run
    history.write.mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"{CATALOG}.silver.security_name_history")

gate = spark.createDataFrame(results, spark.table(f"{CATALOG}.audit.gate_results").schema)
gate.write.mode("append").saveAsTable(f"{CATALOG}.audit.gate_results")
display(gate)
assert passed, "❌ Gate bronze_to_silver (SEC) FAILED. dbt must not run."
print(f"✅ silver.securities: {n_rows} tickers · silver.security_name_history: {history.count()} periods "
      f"for {n_companies} companies · {n_removed} overlapping period(s) resolved")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Look: Apple's name history

# COMMAND ----------

display(spark.table(f"{CATALOG}.silver.security_name_history").where("cik = '0000320193'").orderBy("valid_from"))