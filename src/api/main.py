"""
FastAPI сервер для AI-агента госзакупок.
REST API + WebSocket чат.
"""
import asyncio
import json
import logging
import time
import secrets
from collections import defaultdict
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from src.config import app_config
from src.agent.react_agent import get_agent, ReActAgent
from src.agent.tools import get_data_overview, execute_sql, get_fair_price

logger = logging.getLogger(__name__)

_FRONTEND_DIR = Path(__file__).resolve().parent.parent.parent / "frontend"

app = FastAPI(
    title="AI Госзакупки РК",
    description="AI-агент анализа государственных закупок Республики Казахстан",
    version="1.0.0",
)

_cors_origins: list[str] = []
if app_config.cors_origins:
    _cors_origins = [o.strip() for o in app_config.cors_origins.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=bool(_cors_origins),
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", "X-API-Key"],
)

app.mount("/static", StaticFiles(directory=str(_FRONTEND_DIR)), name="static")

async def verify_api_key(request: Request):
    """If APP_API_KEY is configured, require X-API-Key header."""
    if not app_config.api_key:
        return  # auth disabled — open access
    key = request.headers.get("X-API-Key", "")
    if not secrets.compare_digest(key, app_config.api_key):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")

# WebSocket rate limiter
_MAX_WS_CONNECTIONS = 50
_WS_MSG_PER_MINUTE = 20
_AGENT_CHAT_TIMEOUT = 300.0  # 5 минут


class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, WebSocket] = {}
        self.agents: dict[str, ReActAgent] = {}
        self._msg_timestamps: dict[str, list[float]] = defaultdict(list)

    async def connect(self, websocket: WebSocket, client_id: str) -> bool:
        if len(self.active_connections) >= _MAX_WS_CONNECTIONS:
            await websocket.close(code=1013, reason="Too many connections")
            return False
        await websocket.accept()
        self.active_connections[client_id] = websocket
        self.agents[client_id] = ReActAgent()
        return True

    async def disconnect(self, client_id: str):
        self.active_connections.pop(client_id, None)
        agent = self.agents.pop(client_id, None)
        if agent:
            await agent.close()
        self._msg_timestamps.pop(client_id, None)

    def is_rate_limited(self, client_id: str) -> bool:
        now = time.time()
        timestamps = self._msg_timestamps[client_id]
        self._msg_timestamps[client_id] = [t for t in timestamps if now - t < 60]
        if len(self._msg_timestamps[client_id]) >= _WS_MSG_PER_MINUTE:
            return True
        self._msg_timestamps[client_id].append(now)
        return False

    async def send_message(self, client_id: str, message: dict):
        ws = self.active_connections.get(client_id)
        if ws:
            await ws.send_json(message)

    def get_agent(self, client_id: str) -> ReActAgent:
        if client_id not in self.agents:
            self.agents[client_id] = ReActAgent()
        return self.agents[client_id]


manager = ConnectionManager()

@app.get("/api/health")
async def health():
    """Проверка работоспособности."""
    overview = get_data_overview()
    return {"status": "ok", "data": overview}


@app.get("/api/overview", dependencies=[Depends(verify_api_key)])
async def data_overview():
    """Обзор загруженных данных."""
    return get_data_overview()


@app.post("/api/query", dependencies=[Depends(verify_api_key)])
async def query_data(request: dict):
    """Выполняет SQL запрос (только SELECT)."""
    sql = request.get("query", "")
    return execute_sql(sql)


@app.post("/api/fair-price", dependencies=[Depends(verify_api_key)])
async def fair_price(request: dict):
    """Рассчитывает справедливую цену."""
    enstru = request.get("enstru_code", "")
    region = request.get("region")
    return get_fair_price(enstru, region)


@app.post("/api/chat", dependencies=[Depends(verify_api_key)])
async def chat_endpoint(request: dict):
    """REST endpoint для чата с AI агентом."""
    message = request.get("message", "")
    if not message:
        return {"error": "message is required"}

    agent = get_agent()
    try:
        response = await agent.chat(message)
        metrics = agent.get_metrics()
        return {"response": response, "metrics": metrics}
    except Exception as e:
        logger.error(f"Chat error: {e}")
        return {"error": "Внутренняя ошибка сервера. Попробуйте позже."}

@app.websocket("/ws/chat/{client_id}")
async def websocket_chat(websocket: WebSocket, client_id: str):
    """WebSocket endpoint для real-time чата."""
    if not client_id or len(client_id) > 64:
        await websocket.close(code=1008, reason="Invalid client_id")
        return

    connected = await manager.connect(websocket, client_id)
    if not connected:
        return

    try:
        while True:
            data = await websocket.receive_text()

            try:
                message = json.loads(data)
            except json.JSONDecodeError:
                await manager.send_message(client_id, {
                    "type": "error",
                    "content": "Некорректный формат сообщения.",
                })
                continue

            user_text = message.get("message", "")
            if not user_text:
                continue

            if manager.is_rate_limited(client_id):
                await manager.send_message(client_id, {
                    "type": "error",
                    "content": "Слишком много запросов. Подождите минуту.",
                })
                continue

            await manager.send_message(client_id, {
                "type": "thinking",
                "content": "Анализирую запрос..."
            })

            agent = manager.get_agent(client_id)

            try:
                response = await asyncio.wait_for(
                    agent.chat(user_text),
                    timeout=_AGENT_CHAT_TIMEOUT,
                )

                if not response or not response.strip():
                    response = "Не удалось сформировать ответ. Попробуйте переформулировать вопрос."

                metrics = agent.get_metrics()
                await manager.send_message(client_id, {
                    "type": "response",
                    "content": response,
                    "metrics": metrics,
                })
            except asyncio.TimeoutError:
                logger.error(f"Agent chat timeout for client {client_id}")
                await manager.send_message(client_id, {
                    "type": "error",
                    "content": "Превышено время обработки запроса (5 минут). Попробуйте упростить вопрос.",
                })
            except Exception as e:
                logger.error(f"Agent error for client {client_id}: {e}", exc_info=True)
                await manager.send_message(client_id, {
                    "type": "error",
                    "content": "Произошла ошибка при обработке запроса. Попробуйте ещё раз.",
                })

    except WebSocketDisconnect:
        await manager.disconnect(client_id)
    except Exception as e:
        logger.error(f"WebSocket unexpected error for client {client_id}: {e}", exc_info=True)
        await manager.disconnect(client_id)


@app.get("/")
async def chat_ui():
    """Отдаёт HTML чат-интерфейс."""
    return FileResponse(str(_FRONTEND_DIR / "index.html"))


if __name__ == "__main__":
    import uvicorn
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    uvicorn.run(app, host="0.0.0.0", port=app_config.port)
