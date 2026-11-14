# Databricks notebook source
# MAGIC %md
# MAGIC # _config · environment-aware connection settings
# MAGIC
# MAGIC Every notebook loads this with `%run ./_config`. Nothing environment-specific is written in code:
# MAGIC all connection details live in a **secret scope per environment**.
# MAGIC
# MAGIC | Environment | Secret scope |
# MAGIC |---|---|
# MAGIC | `dev` | `brokerage-dev` |
# MAGIC | `prod` | `brokerage-prod` (later) |
# MAGIC
# MAGIC Each scope holds the same four keys: `mssql_host`, `mssql_database`, `mssql_user`, `mssql_password`.
# MAGIC To switch environment, change the **env** box at the top of the notebook — no code changes.

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
ENV = dbutils.widgets.get("env")
SECRET_SCOPE = f"brokerage-{ENV}"

# COMMAND ----------

def sql_config(scope: str = SECRET_SCOPE) -> dict:
    """Read the Azure SQL connection details for one environment from its secret scope."""
    get = lambda key: dbutils.secrets.get(scope, key)
    return {
        "host": get("mssql_host"),
        "database": get("mssql_database"),
        "user": get("mssql_user"),
        "password": get("mssql_password"),
    }


def jdbc_options(cfg: dict) -> dict:
    """Options for spark.read / df.write with format('jdbc'). Encrypted connection, 60s login timeout
    (the free-tier database can take ~30s to wake up)."""
    url = (f"jdbc:sqlserver://{cfg['host']}:1433;database={cfg['database']};"
           "encrypt=true;trustServerCertificate=false;"
           "hostNameInCertificate=*.database.windows.net;loginTimeout=60")
    return {
        "url": url,
        "user": cfg["user"],
        "password": cfg["password"],
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }


SQL = sql_config()
JDBC = jdbc_options(SQL)
print(f"Environment: {ENV} · secret scope: {SECRET_SCOPE} · database: {SQL['database']}")

# COMMAND ----------

