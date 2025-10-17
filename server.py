import asyncio
import json
import time
from datetime import datetime

import httpx
import websockets
from fastapi import FastAPI, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

# ==============================================================
# ⚙️ CONFIG
# ==============================================================
BINANCE_WS = "wss://stream.binance.com:9443/stream"
BINANCE_API = "https://api.binance.com/api/v3/klines"
MAX_STREAM_PER_CONN = 200
PING_INTERVAL = 30
# ==============================================================

app = FastAPI(title="Crypto SSE Server", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# === GLOBAL STATE ===
latest_prices = {}
daily_stats = {}
subscribers: set[asyncio.Queue] = set()
ws_tasks: dict[str, asyncio.Task] = {}


# ==============================================================
# 🔄 BACKGROUND WEBSOCKET WORKER
# ==============================================================
async def ws_worker(symbol_group: list[str]):
    streams = "/".join([f"{s}@trade/{s}@ticker" for s in symbol_group])
    url = f"{BINANCE_WS}?streams={streams}"
    print(f"🟢 WS worker started for {len(symbol_group)} symbols.")

    while True:
        try:
            async with websockets.connect(url, ping_interval=PING_INTERVAL) as ws:
                async for message in ws:
                    data = json.loads(message)
                    payload = data.get("data", {})
                    event_type = payload.get("e")

                    if event_type == "trade":
                        symbol = payload["s"].lower()
                        price = float(payload["p"])
                        latest_prices[symbol] = price
                        await broadcast_update(symbol, price)

                    elif event_type == "24hrTicker":
                        symbol = payload["s"].lower()
                        daily_stats[symbol] = {
                            "open": float(payload.get("o", 0)),
                            "close": float(payload.get("c", 0)),
                            "change_percent": float(payload.get("P", 0)),
                        }

        except Exception as e:
            print("⚠️ WebSocket worker error:", e)
            print("⏳ Reconnecting in 5s...")
            await asyncio.sleep(5)


# ==============================================================
# 📢 BROADCAST FUNCTION
# ==============================================================
async def broadcast_update(symbol: str, price: float):
    stats = daily_stats.get(symbol, {})
    event = json.dumps({
        "timestamp": int(time.time()),
        "symbol": symbol,
        "price": price,
        "open": stats.get("open", 0),
        "close": stats.get("close", 0),
        "change_percent": stats.get("change_percent", 0)
    })

    dead_queues = []
    for q in list(subscribers):
        try:
            await asyncio.wait_for(q.put(event), timeout=0.5)
        except asyncio.TimeoutError:
            dead_queues.append(q)
    for q in dead_queues:
        subscribers.discard(q)


# ==============================================================
# 🧠 WEBSOCKET STARTUP MANAGER
# ==============================================================
async def start_ws_tasks(symbols: list[str]):
    global ws_tasks
    groups = [symbols[i:i + MAX_STREAM_PER_CONN]
              for i in range(0, len(symbols), MAX_STREAM_PER_CONN)]

    # Cancel old tasks
    for task in ws_tasks.values():
        task.cancel()
    ws_tasks.clear()

    for group in groups:
        key = ",".join(group)
        task = asyncio.create_task(ws_worker(group))
        ws_tasks[key] = task
    print(f"🚀 Spawned {len(groups)} Binance WS tasks for {len(symbols)} symbols.")


# ==============================================================
# 🌐 SSE ENDPOINT
# ==============================================================
@app.get("/events")
async def sse_events(request: Request, symbols: str = Query(...)):
    """
    SSE endpoint — ví dụ:
    /events?symbols=btcusdt,ethusdt,bnbusdt
    """
    symbol_list = [s.strip().lower() for s in symbols.split(",") if s.strip()]
    if not symbol_list:
        return {"error": "Thiếu tham số ?symbols=btcusdt,ethusdt"}

    await start_ws_tasks(symbol_list)

    q = asyncio.Queue(maxsize=1000)
    subscribers.add(q)
    print(f"👥 New client connected — total: {len(subscribers)}")

    async def stream():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    data = await asyncio.wait_for(q.get(), timeout=20)
                    yield f"event: update\ndata: {data}\n\n"
                except asyncio.TimeoutError:
                    yield f": keepalive {time.time()}\n\n"
        finally:
            subscribers.discard(q)
            print(f"👋 Client disconnected — {len(subscribers)} remain.")

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


# ==============================================================
# 📈 HISTORY ENDPOINT (cho chart)
# ==============================================================
@app.get("/history")
async def get_history(
    symbol: str = Query(..., description="vd: btcusdt"),
    interval: str = Query("1h", description="vd: 1m,5m,1h,1d"),
    limit: int = Query(200, description="tối đa 1000"),
    start_time: int | None = None,
    end_time: int | None = None,
):
    params = {
        "symbol": symbol.upper(),
        "interval": interval,
        "limit": min(limit, 1000)
    }
    if start_time:
        params["startTime"] = start_time
    if end_time:
        params["endTime"] = end_time

    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(BINANCE_API, params=params)
        if r.status_code != 200:
            return {"error": f"Không lấy được dữ liệu: {r.text}"}

    data = r.json()
    candles = [
        {
            "time": datetime.utcfromtimestamp(item[0] / 1000).isoformat(),
            "open": float(item[1]),
            "high": float(item[2]),
            "low": float(item[3]),
            "close": float(item[4]),
            "volume": float(item[5])
        }
        for item in data
    ]
    return {"symbol": symbol.lower(), "interval": interval, "count": len(candles), "candles": candles}


# ==============================================================
# 🏠 HOME
# ==============================================================
@app.get("/")
def home():
    return {
        "message": "✅ Crypto SSE Server running on Render",
        "usage": "/events?symbols=btcusdt,ethusdt",
        "history": "/history?symbol=btcusdt&interval=1h&limit=100"
    }


# ==============================================================
# ▶️ ENTRY POINT (Render auto-runs this)
# ==============================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=10000, workers=1)
