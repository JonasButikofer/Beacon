# ingest/stream_finnhub_consumer.py
# Databricks notebook / job task — `dbutils` is pre-injected.
#
# Always-on WebSocket consumer: subscribes to Finnhub trade updates for the
# watchlist and lands them as JSON-lines files in a landing volume. Meant to run
# as a Job task with a continuous trigger — `run()` loops forever and reconnects
# on drop; it does not "complete" under normal operation.
#
# Requires the `websocket-client` package (not preinstalled on serverless —
# add it via %pip install websocket-client or the job's environment deps).
import json
import time

import websocket

FINNHUB_TOKEN = dbutils.secrets.get("beacon", "finnhub_token")
LANDING_PATH = "/Volumes/beacon/bronze/quote_landing"
WATCHLIST = ["SPY", "QQQ", "DIA", "IWM", "XLF", "XLE", "XLK", "XLU", "TLT"]

FLUSH_INTERVAL_SECONDS = 30
FLUSH_MAX_MESSAGES = 500

_buffer = []
_last_flush = time.time()


def flush():
    global _buffer, _last_flush
    if not _buffer:
        _last_flush = time.time()
        return
    ts = int(time.time() * 1000)
    path = f"{LANDING_PATH}/quotes_{ts}.json"
    with open(path, "w") as f:
        for row in _buffer:
            f.write(json.dumps(row) + "\n")
    print(f"Flushed {len(_buffer)} quotes to {path}")
    _buffer = []
    _last_flush = time.time()


def on_message(ws, message):
    global _buffer
    msg = json.loads(message)
    if msg.get("type") != "trade":
        return
    for t in msg.get("data", []):
        _buffer.append(
            {
                "symbol": t["s"],
                "price": t["p"],
                "volume": t.get("v"),
                "trade_time": t["t"],
                "conditions": t.get("c", []),
            }
        )
    if len(_buffer) >= FLUSH_MAX_MESSAGES or time.time() - _last_flush >= FLUSH_INTERVAL_SECONDS:
        flush()


def on_open(ws):
    for symbol in WATCHLIST:
        ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
    print(f"Subscribed to {len(WATCHLIST)} symbols.")


def on_error(ws, error):
    print(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed: {close_status_code} {close_msg}")


def run():
    while True:
        ws = websocket.WebSocketApp(
            f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}",
            on_message=on_message,
            on_open=on_open,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever()
        flush()  # capture anything still buffered before reconnecting
        print("Disconnected — reconnecting in 5s...")
        time.sleep(5)


run()
