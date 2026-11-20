# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Generate trades and cash (the "truth")
# MAGIC
# MAGIC Simulates two years of trading for every account, **one account at a time, day by day**, so the data is
# MAGIC internally consistent:
# MAGIC
# MAGIC | Rule | How |
# MAGIC |---|---|
# MAGIC | Money comes in before it's spent | first deposit on the account's first day; deposits are usable **the next day** |
# MAGIC | No short selling | a sell never exceeds the shares held |
# MAGIC | Cash never negative | a buy only spends cash already there; withdrawals never exceed cash |
# MAGIC | Real prices | every trade price is inside that stock's **real** low–high for the day (split-adjusted) |
# MAGIC | Market hours | executions between 09:30 and 16:00 New York time, stored in UTC |
# MAGIC | Closed accounts | on the close date everything is sold and the cash withdrawn |
# MAGIC
# MAGIC Output: **`sim.trades`** and **`sim.cash_movements`**. These are the *ground truth*. The stream (notebook 11)
# MAGIC sends them as messy events; at the end we check that `fct_trades` matches this truth exactly.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Settings and inputs

# COMMAND ----------

import math
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")    # timestamps are stored and compared in UTC
SEED = 42
START, END = date(2024, 1, 1), date(2025, 12, 31)
SIM = f"{CATALOG}.sim"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SIM}")

# Accounts that are open at some point in the window, with the client's segment (sets deposit size)
accounts = (spark.table(f"{CATALOG}.silver.accounts").alias("a")
    .join(spark.table(f"{CATALOG}.silver.clients").alias("c"), "client_id")
    .where(F.coalesce("a.closed_date", F.lit(END)) >= F.lit(START))
    .select("a.account_id", "a.opened_date", "a.closed_date", "c.segment"))

# Split-adjusted low/high per stock per day (SPY is the benchmark, not traded)
prices = (spark.table(f"{CATALOG}.silver.daily_prices")
    .where("ticker <> 'SPY'")
    .withColumn("factor", F.col("split_adjusted_close") / F.col("close"))
    .select("ticker", "trade_date",
            (F.col("low") * F.col("factor")).alias("low"),
            (F.col("high") * F.col("factor")).alias("high")))

prices_pd = prices.toPandas()
trading_days = sorted(r.trade_date for r in spark.table(f"{CATALOG}.silver.daily_prices")
                      .where("ticker = 'SPY'").select("trade_date").collect())

print(f"{accounts.count():,} accounts · {prices_pd.ticker.nunique()} stocks · {len(trading_days)} trading days "
      f"({trading_days[0]} → {trading_days[-1]})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · The simulation for **one** account
# MAGIC Plain Python: walk through the trading days, keep a running cash balance and a dict of shares held,
# MAGIC and only allow actions that keep both valid. Spark runs this function for 2,000 accounts in parallel.

# COMMAND ----------

DEPOSIT_RANGE = {"Mass": (5_000, 50_000), "Affluent": (50_000, 500_000), "HNW": (500_000, 5_000_000)}
TRADES_PER_DAY = 0.45            # average; ~225 trades per account over two years
TOP_UP_PROB = 1 / 63             # roughly one extra deposit a quarter
WITHDRAW_PROB = 1 / 250          # roughly one withdrawal a year
SEC_FEE_RATE = 0.0000278         # regulatory fee on sells (per $ of proceeds)
ET, UTC = ZoneInfo("America/New_York"), ZoneInfo("UTC")

PX = {(t, d): (lo, hi) for t, d, lo, hi in prices_pd.itertuples(index=False)}
ALL_TICKERS = sorted(prices_pd.ticker.unique())

OUT_SCHEMA = ("kind string, id bigint, account_id int, ticker string, side string, quantity int, "
              "price double, fees double, txn_type string, amount double, event_date date, event_time timestamp")


def market_times(rng, d, n):
    """n sorted execution times between 09:30 and 16:00 New York time, returned in UTC."""
    open_et = datetime.combine(d, time(9, 30), ET)
    secs = np.sort(rng.integers(0, int(6.5 * 3600), size=n))
    return [(open_et + timedelta(seconds=int(s))).astimezone(UTC) for s in secs]


def simulate_account(pdf: pd.DataFrame) -> pd.DataFrame:
    a = pdf.iloc[0]
    acct = int(a.account_id)
    rng = np.random.default_rng([SEED, acct])                  # same account → same history, every run
    last_day = min(a.closed_date, END) if pd.notna(a.closed_date) else END
    days = [d for d in trading_days if max(a.opened_date, START) <= d <= last_day]
    if not days:
        return pd.DataFrame(columns=[c.split()[0] for c in OUT_SCHEMA.split(", ")])

    rows, cash, pending, held = [], 0.0, 0.0, {}
    favourites = list(rng.choice(ALL_TICKERS, size=int(rng.integers(5, 16)), replace=False))
    lo_dep, hi_dep = DEPOSIT_RANGE[a.segment]
    seq = {"trade": 0, "cash": 0}

    def cash_row(d, txn_type, amount):
        seq["cash"] += 1
        rows.append(("cash", acct * 100_000 + seq["cash"], acct, None, None, None, None, None,
                     txn_type, round(amount, 2), d, None))

    def trade_row(d, t, ticker, side, qty, price, fees):
        seq["trade"] += 1
        rows.append(("trade", acct * 100_000 + seq["trade"], acct, ticker, side, int(qty), price, fees,
                     None, None, d, t))

    for i, d in enumerate(days):
        cash, pending = round(cash + pending, 2), 0.0          # yesterday's deposits are now usable
        closing = pd.notna(a.closed_date) and i == len(days) - 1   # last trading day of a closed account

        if i == 0:                                             # opening deposit (log-uniform size)
            pending = round(math.exp(rng.uniform(math.log(lo_dep), math.log(hi_dep))), 2)
            cash_row(d, "DEPOSIT", pending)
            continue

        # Trades: buy from favourites with spare cash, or sell something held
        n = 0 if closing else int(rng.poisson(TRADES_PER_DAY))
        for t in market_times(rng, d, n):
            sellable = [k for k, q in held.items() if q > 0 and (k, d) in PX]
            if sellable and (cash < 500 or rng.random() < 0.4):
                tk = sellable[int(rng.integers(len(sellable)))]
                lo, hi = PX[(tk, d)]
                price = min(max(round(rng.uniform(lo, hi), 2), lo), hi)
                qty = held[tk] if rng.random() < 0.3 else int(rng.integers(1, held[tk] + 1))
                proceeds = round(qty * price, 2)
                fee = math.ceil(proceeds * SEC_FEE_RATE * 100) / 100
                held[tk] -= qty
                cash = round(cash + proceeds - fee, 2)
                trade_row(d, t, tk, "SELL", qty, price, fee)
            else:
                buyable = [k for k in favourites if (k, d) in PX]
                if not buyable:
                    continue
                tk = buyable[int(rng.integers(len(buyable)))]
                lo, hi = PX[(tk, d)]
                price = min(max(round(rng.uniform(lo, hi), 2), lo), hi)
                qty = int(cash * rng.uniform(0.05, 0.25) // price)
                if qty < 1:
                    continue
                held[tk] = held.get(tk, 0) + qty
                cash = round(cash - qty * price, 2)
                trade_row(d, t, tk, "BUY", qty, price, 0.0)

        if closing:                                            # sell everything, then empty the account
            open_pos = [(k, q) for k, q in held.items() if q > 0 and (k, d) in PX]
            for (tk, qty), t in zip(open_pos, market_times(rng, d, len(open_pos))):
                lo, hi = PX[(tk, d)]
                price = min(max(round(rng.uniform(lo, hi), 2), lo), hi)
                proceeds = round(qty * price, 2)
                fee = math.ceil(proceeds * SEC_FEE_RATE * 100) / 100
                held[tk] = 0
                cash = round(cash + proceeds - fee, 2)
                trade_row(d, t, tk, "SELL", qty, price, fee)
            if cash > 0:
                cash_row(d, "WITHDRAWAL", cash)
                cash = 0.0
            break

        # End-of-day cash movements
        if rng.random() < TOP_UP_PROB and i < len(days) - 5:
            pending = round(math.exp(rng.uniform(math.log(lo_dep), math.log(hi_dep))) * rng.uniform(0.05, 0.2), 2)
            cash_row(d, "DEPOSIT", pending)
        if rng.random() < WITHDRAW_PROB and cash > 1_000:
            amount = round(cash * rng.uniform(0.1, 0.3), 2)
            cash = round(cash - amount, 2)
            cash_row(d, "WITHDRAWAL", amount)

    out = pd.DataFrame(rows, columns=[c.split()[0] for c in OUT_SCHEMA.split(", ")])
    out["event_time"] = pd.to_datetime(out["event_time"], utc=True)   # explicit UTC, whatever the cluster's timezone
    return out.astype({"quantity": "Int32"})                   # whole numbers that may be empty (cash rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Run it for every account (in parallel), check it, save it

# COMMAND ----------

generated = (accounts.repartition(16, "account_id")
             .groupBy("account_id").applyInPandas(simulate_account, OUT_SCHEMA)
             .cache())

trades = (generated.where("kind = 'trade'")
          .select(F.col("id").alias("trade_id"), "account_id", "ticker", "side", "quantity", "price", "fees",
                  F.col("event_time").alias("executed_at"), F.col("event_date").alias("trade_date")))
cash = (generated.where("kind = 'cash'")
        .select(F.col("id").alias("cash_txn_id"), "account_id", "txn_type", "amount",
                F.col("event_date").alias("effective_date")))

# --- Checks: the truth must be valid before anything is built on it ---
signed_qty = F.when(F.col("side") == "BUY", F.col("quantity")).otherwise(-F.col("quantity"))
w_pos = Window.partitionBy("account_id", "ticker").orderBy("executed_at", "trade_id")
short_sells = trades.withColumn("held_after", F.sum(signed_qty).over(w_pos)).where("held_after < 0").count()

flows = (trades.select("account_id", F.col("trade_date").alias("d"),
                       F.when(F.col("side") == "BUY", -F.col("quantity") * F.col("price"))
                        .otherwise(F.col("quantity") * F.col("price") - F.col("fees")).alias("amt"))
         .unionByName(cash.select("account_id", F.col("effective_date").alias("d"),
                                  F.when(F.col("txn_type") == "DEPOSIT", F.col("amount"))
                                   .otherwise(-F.col("amount")).alias("amt"))))
w_cash = Window.partitionBy("account_id").orderBy("d")
negative_cash = (flows.groupBy("account_id", "d").agg(F.sum("amt").alias("amt"))
                 .withColumn("balance", F.sum("amt").over(w_cash))
                 .where("balance < -0.005").count())

outside_range = (trades.join(prices, (trades.ticker == prices.ticker) & (trades.trade_date == prices.trade_date))
                 .where("price < low - 1e-9 OR price > high + 1e-9").count())

et_time = F.date_format(F.from_utc_timestamp("executed_at", "America/New_York"), "HH:mm:ss")
outside_hours = trades.where((et_time < "09:30:00") | (et_time > "16:00:00")).count()

n_trades, n_cash = trades.count(), cash.count()
checks = {
    "trades generated": n_trades > 0,
    "no short sells": short_sells == 0,
    "cash never negative (end of day)": negative_cash == 0,
    "every price inside the day's low–high": outside_range == 0,
    "every execution in market hours": outside_hours == 0,
    "trade_id unique": trades.select("trade_id").distinct().count() == n_trades,
}
for name, ok in checks.items():
    print(("✅" if ok else "❌"), name)
assert all(checks.values()), "Generator checks failed: nothing saved."

trades.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SIM}.trades")
cash.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SIM}.cash_movements")
print(f"✅ Saved {n_trades:,} trades and {n_cash:,} cash movements for "
      f"{trades.select('account_id').distinct().count():,} accounts")

# COMMAND ----------

display(spark.table(f"{SIM}.trades").groupBy("side").agg(
    F.count("*").alias("trades"), F.round(F.sum(F.col("quantity") * F.col("price")), 0).alias("value_usd")))