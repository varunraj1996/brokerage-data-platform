# Databricks notebook source
# MAGIC %md
# MAGIC # 14 · Live prices: producer (Finnhub websocket → JSON files)
# MAGIC
# MAGIC **REST vs websocket**
# MAGIC - **REST** (Tiingo, SEC): you ask, you get one answer, the connection closes.
# MAGIC - **Websocket** (Finnhub live feed): the connection **stays open** and Finnhub **pushes** every new trade the
# MAGIC   moment it happens.
# MAGIC
# MAGIC This notebook listens for a set number of minutes and writes what it hears to the Volume as small JSON files,
# MAGIC **one file every 10 seconds**. Notebook 15 streams those files with Auto Loader, exactly like the trade events.
# MAGIC
# MAGIC | Setting | Value | Why |
# MAGIC |---|---|---|
# MAGIC | Symbols | 28 stocks + SPY + Bitcoin = **30** | Finnhub's free limit is 30; Bitcoin trades 24/7, so it can be tested any time |
# MAGIC | Market hours | stocks 09:30–16:00 New York time, weekdays | outside these hours only Bitcoin ticks arrive |
# MAGIC | Reconnects | automatic, waiting 1, 2, 4 … up to 60 s | a live connection *will* drop now and then; the producer must survive it |
# MAGIC | Session log | `audit.live_sessions` | ticks, files and reconnects per session: the numbers for the README |
# MAGIC
# MAGIC **Needs:** cluster library `websocket-client` (PyPI) and the secret `finnhub_api_key`.
# MAGIC ⚠️ The key travels inside the connection URL: never print `ws` or the URL.
# MAGIC
# MAGIC Raw ticks stay private (Finnhub's terms): they live only in the Volume, never in the repo.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import json
import time
import uuid
from collections import Counter
from datetime import datetime, timezone

import requests
import websocket

FINNHUB_KEY = dbutils.secrets.get(SECRET_SCOPE, "finnhub_api_key")

dbutils.widgets.text("minutes", "2", "Run for (minutes)")
MINUTES = float(dbutils.widgets.get("minutes"))

SYMBOLS = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "AMD", "NFLX",
           "JPM", "BAC", "GS", "V", "MA", "UNH", "LLY", "JNJ", "XOM", "CVX",
           "WMT", "COST", "HD", "KO", "PEP", "DIS", "BA", "CAT",
           "SPY", "BINANCE:BTCUSDT"]
LANDING = f"/Volumes/{CATALOG}/bronze/raw/live_ticks"
STAGING = f"/Volumes/{CATALOG}/bronze/raw/_staging/live_ticks"     # files are written here first, then moved
SESSIONS = f"{CATALOG}.audit.live_sessions"
FLUSH_SECONDS = 10
SESSION_ID = str(uuid.uuid4())

spark.sql(f"""CREATE TABLE IF NOT EXISTS {SESSIONS} (
    session_id STRING, started_at TIMESTAMP, ended_at TIMESTAMP, minutes DOUBLE, symbols INT,
    ticks BIGINT, files BIGINT, reconnects INT, symbols_with_ticks INT)""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Quick check: is the key valid?

# COMMAND ----------

r = requests.get("https://finnhub.io/api/v1/quote", params={"symbol": "AAPL"},
                 headers={"X-Finnhub-Token": FINNHUB_KEY}, timeout=10)
print(r.status_code, r.json())     # c = current price, pc = previous close, t = time
assert r.status_code == 200, "Key not accepted: check the finnhub_api_key secret"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Listen, buffer, write a file every 10 seconds
# MAGIC Each line in a file is one trade: `symbol, price, volume, trade_ts_ms` (exchange time, milliseconds since 1970),
# MAGIC `conditions`, `received_at` (when we got it, UTC) and `session_id`.
# MAGIC The gap between exchange time and `received_at` is the **feed latency** we'll measure later.

# COMMAND ----------

def connect():
    ws = websocket.create_connection(f"wss://ws.finnhub.io?token={FINNHUB_KEY}", timeout=5)
    for s in SYMBOLS:
        ws.send(json.dumps({"type": "subscribe", "symbol": s}))
    return ws


def flush(buffer):
    """Write the buffered ticks as one JSON-lines file in today's folder (UTC)."""
    now = datetime.now(timezone.utc)
    name = f"date={now:%Y-%m-%d}/ticks_{now:%H%M%S_%f}_{SESSION_ID[:8]}.json"
    staging, final = f"{STAGING}/{name}", f"{LANDING}/{name}"
    import contextlib, io
    with contextlib.redirect_stdout(io.StringIO()):          # hide dbutils' "Wrote N bytes." message
        # Write somewhere Auto Loader doesn't look, then MOVE it into the landing folder. A move (rename) is
        # all-or-nothing, so Auto Loader can never see a half-written file.
        dbutils.fs.put(staging, "\n".join(json.dumps(r) for r in buffer), overwrite=False)
        dbutils.fs.mv(staging, final)


started = datetime.now(timezone.utc)
stats = {"ticks": 0, "files": 0, "reconnects": 0}
per_symbol = Counter()
buffer, backoff = [], 1
end_at = time.time() + MINUTES * 60
last_flush = last_report = time.time()
ticks_at_last_report = 0
ws = connect()
print(f"Listening to {len(SYMBOLS)} symbols for {MINUTES:g} min · session {SESSION_ID[:8]}")

try:
    while time.time() < end_at:
        raw = None
        try:
            raw = ws.recv()
            backoff = 1
        except websocket.WebSocketTimeoutException:
            pass                                               # quiet for 5 s: normal outside market hours
        except Exception as e:                                 # connection dropped: wait, then reconnect
            stats["reconnects"] += 1
            print(f"⚠️ connection lost ({type(e).__name__}); reconnecting in {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            try:
                ws.close()
            except Exception:
                pass
            try:
                ws = connect()
            except Exception as e2:
                print(f"   reconnect failed ({type(e2).__name__}); will retry")
            continue

        if raw:
            msg = json.loads(raw)
            if msg.get("type") == "trade":
                received_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
                for t in msg["data"]:
                    buffer.append({"symbol": t["s"], "price": t["p"], "volume": t["v"], "trade_ts_ms": t["t"],
                                   "conditions": t.get("c"), "received_at": received_at, "session_id": SESSION_ID})
                    per_symbol[t["s"]] += 1
                    stats["ticks"] += 1

        if buffer and time.time() - last_flush >= FLUSH_SECONDS:
            flush(buffer)
            stats["files"] += 1
            buffer, last_flush = [], time.time()

        if time.time() - last_report >= 60:                   # a progress line once a minute
            print(f"{datetime.now(timezone.utc):%H:%M:%S} UTC · {stats['ticks'] - ticks_at_last_report:,} ticks "
                  f"in the last minute · {stats['files']} files · {stats['reconnects']} reconnects")
            ticks_at_last_report, last_report = stats["ticks"], time.time()
finally:                                                       # runs even if you press Cancel
    if buffer:
        flush(buffer)
        stats["files"] += 1
    try:
        ws.close()
    except Exception:
        pass
    ended = datetime.now(timezone.utc)
    (spark.createDataFrame([(SESSION_ID, started, ended, (ended - started).total_seconds() / 60, len(SYMBOLS),
                             stats["ticks"], stats["files"], stats["reconnects"], len(per_symbol))],
                           spark.table(SESSIONS).schema)
     .write.mode("append").saveAsTable(SESSIONS))
    print(f"✅ Session {SESSION_ID[:8]}: {stats['ticks']:,} ticks from {len(per_symbol)} symbols · "
          f"{stats['files']} files · {stats['reconnects']} reconnects")
    print("Busiest:", per_symbol.most_common(5))

# COMMAND ----------

display(spark.table(SESSIONS).orderBy("started_at", ascending=False).limit(10))