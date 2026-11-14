# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 · Generate source data (Spark, parallel)
# MAGIC
# MAGIC Generates a **synthetic** brokerage dataset with Spark and loads it into the **Azure SQL source** (`brokerage_oltp`).
# MAGIC
# MAGIC | Table | Rows (default) |
# MAGIC |---|---|
# MAGIC | `brokerage.advisors` | 25 |
# MAGIC | `brokerage.clients` | 1,000 |
# MAGIC | `brokerage.accounts` | 2,000 |
# MAGIC
# MAGIC **How the parallelism works**
# MAGIC - **Generate in parallel:** rows are built by `mapInPandas`, so every Spark partition builds its own slice of rows at the same time.
# MAGIC - **Deterministic in parallel:** each row's random values are seeded from `SEED + row id`, never from a shared random generator, so the data is identical however Spark splits the work.
# MAGIC - **Write in parallel:** `WRITE_PARTITIONS` connections insert in batches of `BATCH_SIZE`. Kept small on purpose: the Azure SQL free tier has 2 vCores, and more connections would only queue up.
# MAGIC - **Read in parallel:** the final check uses a partitioned JDBC read (`partitionColumn` / `numPartitions`).
# MAGIC
# MAGIC **Re-runnable:** each run wipes the three tables (including client history) and reloads them.
# MAGIC **Synthetic only:** names come from Faker; SSNs use the never-issued 900–999 range.

# COMMAND ----------

# MAGIC %md
# MAGIC **Requires** the `faker` library installed on the cluster (Compute → cluster → Libraries → PyPI → `faker`).

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Settings

# COMMAND ----------

from datetime import date

import pandas as pd
from pyspark.sql import functions as F

# Dataset size: raise these to scale up; the code doesn't change
N_ADVISORS = 25
N_CLIENTS = 1_000
N_ACCOUNTS = 2_000
SEED = 42

# Parallelism
GEN_PARTITIONS = 8        # Spark tasks that build rows at the same time
WRITE_PARTITIONS = 4      # simultaneous connections to Azure SQL (free tier = 2 vCores, so keep this small)
BATCH_SIZE = 5_000        # rows sent per INSERT batch

DATA_END = date(2025, 12, 31)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row builders
# MAGIC Each function receives batches of row ids (a pandas DataFrame per batch) and returns finished rows.
# MAGIC Spark calls them on many partitions at once.

# COMMAND ----------

TEAMS = ["Boston East", "Boston West", "New York", "Chicago", "San Francisco"]
STATES = {"MA": 30, "NY": 20, "NJ": 10, "CT": 10, "IL": 10, "CA": 10, "TX": 10}
RISK = {"Conservative": 30, "Moderate": 50, "Aggressive": 20}
SEGMENT = {"Mass": 50, "Affluent": 35, "HNW": 15}
ACCOUNT_TYPE = {"Individual": 45, "Joint": 20, "IRA": 25, "Roth IRA": 10}


def rng_for(table_salt: int, row_id: int):
    """A random generator unique to one row: same row id -> same values, on any partition."""
    import random
    return random.Random(SEED * 1_000_003 + table_salt * 10_007 + int(row_id))


def faker_for(rng):
    from faker import Faker
    fake = Faker("en_US")
    fake.seed_instance(rng.getrandbits(32))
    return fake


def rand_date(rng, start: date, end: date) -> date:
    from datetime import timedelta
    return start + timedelta(days=rng.randint(0, (end - start).days))


def weighted(rng, options: dict):
    return rng.choices(list(options), weights=list(options.values()))[0]


def build_advisors(batches):
    for ids in batches:
        rows = []
        for i in ids["id"]:
            rng = rng_for(1, i); fake = faker_for(rng)
            first, last = fake.first_name(), fake.last_name()
            rows.append({
                "advisor_id": int(i),
                "full_name": f"{first} {last}",
                "email": f"{first}.{last}.{i}@brokerage-demo.com".lower(),
                "team": rng.choice(TEAMS),
                "hired_date": rand_date(rng, date(2012, 1, 1), date(2022, 12, 31)),
            })
        yield pd.DataFrame(rows)


def build_clients(batches):
    for ids in batches:
        rows = []
        for i in ids["id"]:
            rng = rng_for(2, i); fake = faker_for(rng)
            first, last = fake.first_name(), fake.last_name()
            rows.append({
                "client_id": int(i),
                "first_name": first,
                "last_name": last,
                "email": f"{first}.{last}.{i}@example.com".lower(),
                "phone": fake.numerify("###-###-####"),
                "ssn": f"{rng.randint(900, 999)}-{rng.randint(10, 99)}-{rng.randint(1000, 9999)}",
                "date_of_birth": rand_date(rng, date(1950, 1, 1), date(2000, 12, 31)),
                "state": weighted(rng, STATES),
                "risk_profile": weighted(rng, RISK),
                "segment": weighted(rng, SEGMENT),
                "advisor_id": rng.randint(1, N_ADVISORS),
                "onboarded_date": rand_date(rng, date(2018, 1, 1), date(2023, 12, 31)),
            })
        yield pd.DataFrame(rows)


def build_accounts(batches):
    """Input rows already carry account_id, client_id and the client's onboarded_date (joined in Spark)."""
    from datetime import timedelta
    for part in batches:
        rows = []
        for r in part.itertuples(index=False):
            rng = rng_for(3, r.account_id)
            opened = rand_date(rng, r.onboarded_date, date(2024, 6, 30))       # never before the client joined
            closed = rand_date(rng, opened + timedelta(days=30), DATA_END) if rng.random() < 0.05 else None
            rows.append({
                "account_id": int(r.account_id),
                "account_number": f"BRK{int(r.account_id):07d}",
                "client_id": int(r.client_id),
                "account_type": weighted(rng, ACCOUNT_TYPE),
                "status": "Closed" if closed else "Open",
                "opened_date": opened,
                "closed_date": closed,
            })
        yield pd.DataFrame(rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate in parallel

# COMMAND ----------

ADVISOR_SCHEMA = "advisor_id int, full_name string, email string, team string, hired_date date"
CLIENT_SCHEMA = ("client_id int, first_name string, last_name string, email string, phone string, ssn string, "
                 "date_of_birth date, state string, risk_profile string, segment string, advisor_id int, "
                 "onboarded_date date")
ACCOUNT_SCHEMA = ("account_id int, account_number string, client_id int, account_type string, status string, "
                  "opened_date date, closed_date date")

advisors = spark.range(1, N_ADVISORS + 1, numPartitions=1).mapInPandas(build_advisors, ADVISOR_SCHEMA)
clients = spark.range(1, N_CLIENTS + 1, numPartitions=GEN_PARTITIONS).mapInPandas(build_clients, CLIENT_SCHEMA)

# Account owners: every client gets account #client_id; the remaining accounts get a client chosen by a
# hash of the account id (deterministic, computed in parallel by Spark itself).
owners = (spark.range(1, N_ACCOUNTS + 1, numPartitions=GEN_PARTITIONS)
          .withColumnRenamed("id", "account_id")
          .withColumn("client_id", F.when(F.col("account_id") <= N_CLIENTS, F.col("account_id"))
                      .otherwise(F.pmod(F.xxhash64(F.lit(SEED), F.col("account_id")), F.lit(N_CLIENTS)) + 1)
                      .cast("int")))
accounts = (owners.join(clients.select("client_id", "onboarded_date"), "client_id")
            .select("account_id", "client_id", "onboarded_date")
            .mapInPandas(build_accounts, ACCOUNT_SCHEMA))

# Materialise once so generation isn't repeated for the write and the checks
advisors, clients, accounts = advisors.cache(), clients.cache(), accounts.cache()
print(f"Generated: {advisors.count()} advisors, {clients.count()} clients, {accounts.count()} accounts")
display(accounts.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the generated data before loading
# MAGIC Catch problems here, before Azure SQL's constraints reject a half-finished load.

# COMMAND ----------

checks = {
    "advisor emails unique": advisors.select("email").distinct().count() == N_ADVISORS,
    "client emails unique": clients.select("email").distinct().count() == N_CLIENTS,
    "account numbers unique": accounts.select("account_number").distinct().count() == N_ACCOUNTS,
    "every client has an account": accounts.select("client_id").distinct().count() == N_CLIENTS,
    "accounts open after client joined": accounts.join(clients, "client_id")
        .filter(F.col("opened_date") < F.col("onboarded_date")).count() == 0,
    "closed accounts have a close date": accounts.filter(
        (F.col("status") == "Closed") != F.col("closed_date").isNotNull()).count() == 0,
}
for name, ok in checks.items():
    print(("✅ " if ok else "❌ ") + name)
assert all(checks.values()), "Generated data failed a check; nothing was loaded"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load into Azure SQL
# MAGIC 1. **Wipe:** Spark can't run `DELETE` directly, so the wipe runs as the JDBC `sessionInitStatement` of a tiny read.
# MAGIC    Children go first (accounts → clients → advisors) so foreign keys never break; client history is cleared by switching
# MAGIC    temporal versioning off and on.
# MAGIC 2. **Insert** parents first, `WRITE_PARTITIONS` connections in parallel, `BATCH_SIZE` rows per batch.

# COMMAND ----------

WIPE_SQL = """
DELETE FROM brokerage.accounts;

IF EXISTS (SELECT 1 FROM sys.tables
           WHERE object_id = OBJECT_ID('brokerage.clients') AND temporal_type = 2)
    ALTER TABLE brokerage.clients SET (SYSTEM_VERSIONING = OFF);

EXEC('DELETE FROM brokerage.clients_history');
EXEC('DELETE FROM brokerage.clients');
EXEC('ALTER TABLE brokerage.clients SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = brokerage.clients_history))');

DELETE FROM brokerage.advisors;
"""

(spark.read.format("jdbc")
 .options(**JDBC)
 .option("query", "SELECT 1 AS wiped")
 .option("sessionInitStatement", WIPE_SQL)
 .load().collect())
print("🧹 Wiped")


def write(df, table):
    (df.repartition(WRITE_PARTITIONS)
       .write.format("jdbc")
       .options(**JDBC)
       .option("dbtable", f"brokerage.{table}")
       .option("batchsize", BATCH_SIZE)
       .mode("append")
       .save())
    print(f"⬆️  brokerage.{table}")


write(advisors, "advisors")
write(clients, "clients")      # valid_from / valid_to are filled in by SQL Server
write(accounts, "accounts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check: Azure SQL holds exactly what was generated
# MAGIC Read back **in parallel** (`numPartitions` connections, each reading a slice of ids) and compare counts.

# COMMAND ----------

def read_parallel(table, key, upper):
    return (spark.read.format("jdbc")
            .options(**JDBC)
            .option("dbtable", f"brokerage.{table}")
            .option("partitionColumn", key)       # split the read by this column…
            .option("lowerBound", 1)
            .option("upperBound", upper)
            .option("numPartitions", WRITE_PARTITIONS)   # …into this many parallel slices
            .load())


expected = {("advisors", "advisor_id"): N_ADVISORS,
            ("clients", "client_id"): N_CLIENTS,
            ("accounts", "account_id"): N_ACCOUNTS}

for (table, key), n in expected.items():
    actual = read_parallel(table, key, n).count()
    print(("✅" if actual == n else "❌") + f" brokerage.{table}: expected {n:,}, found {actual:,}")
    assert actual == n, f"Row count mismatch in brokerage.{table}"

# COMMAND ----------

