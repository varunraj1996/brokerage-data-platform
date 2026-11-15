# Start-of-session setup for dbt in the Databricks web terminal.
# Usage:  source dbt_env.sh      (asks for your token; nothing secret is stored)

if [ ! -f ~/dbt-venv/bin/activate ]; then
    echo "Creating dbt environment (first run after a cluster start, ~1 min)..."
    /databricks/python3/bin/pip install --quiet virtualenv
    /databricks/python3/bin/python -m virtualenv --quiet ~/dbt-venv
    ~/dbt-venv/bin/pip install --quiet "dbt-databricks>=1.10,<2"
fi
source ~/dbt-venv/bin/activate

cd /Workspace/Users/varunrajvavilala@gmail.com/brokerage-data-platform/dbt_brokerage

export DBT_DATABRICKS_HOST="adb-3256422244682962.2.azuredatabricks.net"
export DBT_DATABRICKS_HTTP_PATH="sql/protocolv1/o/3256422244682962/1114-201749-d0v85g2q"

if [ -z "$DBT_DATABRICKS_TOKEN" ]; then
    read -s -p "Databricks token: " DBT_DATABRICKS_TOKEN; echo
    export DBT_DATABRICKS_TOKEN
fi

echo "✅ dbt ready"
