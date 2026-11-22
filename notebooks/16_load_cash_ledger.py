# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 16 · Load the cash ledger into Azure SQL
# MAGIC
# MAGIC The generator (notebook 10) produced deposits and withdrawals that match the trades: money arrives before it's
# MAGIC spent and cash never goes negative. In a real brokerage these live in the **operational database**, so we put them
# MAGIC there: `brokerage.cash_ledger` in Azure SQL (created by `sql/04_cash_ledger.sql`).
# MAGIC From there they flow like every other Azure SQL table: bronze (03) → silver (04) → dbt.
# MAGIC
# MAGIC | Step | What happens |
# MAGIC |---|---|
# MAGIC | 1 · Prepare | read `sim.cash_movements`; amounts as `DECIMAL(14,2)` (exact cents) |
# MAGIC | 2 · Wipe + insert | empty the table, then insert with 4 parallel connections |
# MAGIC | 3 · Check | Azure SQL row count **and** total amount per type = what was generated |

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql import functions as F

WRITE_PARTITIONS = 4
BATCH_SIZE = 5000

cash = (spark.table(f"{CATALOG}.sim.cash_movements")
        .select("cash_txn_id", "account_id", "txn_type",
                F.col("amount").cast("decimal(14,2)").alias("amount"), "effective_date"))
expected = {r.txn_type: (r.n, r.total) for r in
            cash.groupBy("txn_type").agg(F.count("*").alias("n"), F.sum("amount").alias("total")).collect()}
print("Generated:", {k: f"{n:,} rows · ${t:,.2f}" for k, (n, t) in expected.items()})

# COMMAND ----------

# Wipe: Spark can't run DELETE directly, so it runs as the sessionInitStatement of a tiny read
(spark.read.format("jdbc").options(**JDBC)
 .option("query", "SELECT 1 AS wiped")
 .option("sessionInitStatement", "DELETE FROM brokerage.cash_ledger;")
 .load().collect())
print("🧹 Wiped brokerage.cash_ledger")

(cash.repartition(WRITE_PARTITIONS)
 .write.format("jdbc").options(**JDBC)
 .option("dbtable", "brokerage.cash_ledger")
 .option("batchsize", BATCH_SIZE)
 .mode("append")
 .save())
print("⬆️  brokerage.cash_ledger")

# COMMAND ----------

# Check: count and total per type, computed by SQL Server itself
actual = {r.txn_type: (r.n, r.total) for r in
          (spark.read.format("jdbc").options(**JDBC)
           .option("query", "SELECT txn_type, COUNT(*) AS n, SUM(amount) AS total "
                            "FROM brokerage.cash_ledger GROUP BY txn_type")
           .load().collect())}

for txn_type, (n, total) in expected.items():
    a_n, a_total = actual.get(txn_type, (0, 0))
    ok = n == a_n and total == a_total
    print(("✅" if ok else "❌") + f" {txn_type}: generated {n:,} · ${total:,.2f} | Azure SQL {a_n:,} · ${a_total:,.2f}")
    assert ok, f"Mismatch for {txn_type}"