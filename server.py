import asyncio
import json
import time
import websockets
from fastapi import FastAPI, Request, Query
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

# ======== CẤU HÌNH ========
MAX_STREAM_PER_CONN = 200
BINANCE_WS = "wss://stream.binance.com:9443/stream"
# ===========================

latest_prices = {}
subscribers = set()
ws_tasks = {}

# ===========================


async def ws_worker(symbol_group):
    """Worker kết nối Binance WebSocket cho 1 nhóm symbol."""
    streams = "/".join(f"{s}@trade" for s in symbol_group)
    url = f"{BINANCE_WS}?streams={streams}"
    print(f"🔌 WS group: {url}")

    while True:
        try:
            async with websockets.connect(url) as ws:
                async for msg in ws:
                    data = json.loads(msg)
                    payload = data.get("data", {})
                    symbol = payload.get("s", "").lower()
                    price = float(payload.get("p", 0))
                    if not symbol or not price:
                        continue

                    old_price = latest_prices.get(symbol)
                    if old_price != price:
                        latest_prices[symbol] = price
                        event = json.dumps({
                            "timestamp": int(time.time()),
                            "symbol": symbol,
                            "price": price
                        })
                        # Gửi đến tất cả subscriber
                        for q in list(subscribers):
                            await q.put(event)
        except Exception as e:
            print("⚠️ WS lỗi:", e)
            print("⏳ Reconnect sau 5s...")
            await asyncio.sleep(5)


async def start_ws_tasks(symbols):
    """Chia nhóm và tạo task WebSocket"""
    global ws_tasks
    groups = [
        symbols[i:i + MAX_STREAM_PER_CONN]
        for i in range(0, len(symbols), MAX_STREAM_PER_CONN)
    ]
    # Dọn dẹp task cũ
    for task in ws_tasks.values():
        task.cancel()
    ws_tasks.clear()

    for group in groups:
        key = ",".join(group)
        task = asyncio.create_task(ws_worker(group))
        ws_tasks[key] = task
    print(f"🚀 Đã tạo {len(groups)} WS connection cho {len(symbols)} mã crypto")


@app.get("/events")
async def sse_events(request: Request, symbols: str = Query(..., description="Comma-separated crypto symbols")):
    """
    SSE endpoint — nhận danh sách mã crypto từ query, ví dụ:
    /events?symbols=btcusdt,ethusdt,bnbusdt
    """
    # Parse symbols
    symbol_list = [s.strip().lower() for s in symbols.split(",") if s.strip()]
    if not symbol_list:
        return {"error": "Vui lòng truyền ít nhất 1 mã crypto, ví dụ ?symbols=btcusdt,ethusdt"}

    # Khởi chạy task WS nếu chưa có
    await start_ws_tasks(symbol_list)

    # Tạo hàng đợi SSE riêng
    q = asyncio.Queue()
    subscribers.add(q)
    print(f"👥 Client kết nối, tổng: {len(subscribers)}")

    async def stream():
        try:
            while True:
                if await request.is_disconnected():
                    print("❌ Client ngắt kết nối.")
                    break
                try:
                    data = await asyncio.wait_for(q.get(), timeout=20)
                    yield f"event: update\ndata: {data}\n\n"
                except asyncio.TimeoutError:
                    yield f": keepalive {time.time()}\n\n"
        finally:
            subscribers.discard(q)
            print(f"👋 Client rời đi, còn lại: {len(subscribers)}")

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*"
        },
    )


@app.get("/")
def home():
    return {
        "message": "✅ Binance SSE server is running",
        "usage": "/events?symbols=btcusdt,ethusdt,bnbusdt",
        "example": "/events?symbols=btc,eth"
    }
