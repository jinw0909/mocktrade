import logging

from fastapi import WebSocket
from typing import Dict, List
from fastapi.encoders import jsonable_encoder
from starlette.websockets import WebSocketDisconnect

logger = logging.getLogger("connection_manager")

class ConnectionManager:
    def __init__(self):
        # user_id -> list of WebSocket objects
        self.active: Dict[str, List[WebSocket]] = {}

    async def connect(self, user_id:str, ws: WebSocket):
        # Close any existing connections
        for existing_ws in self.active.get(user_id, []):
            try:
                await existing_ws.close(code=1000)
                logger.info(f"[connect] Closed previous connection for user {user_id}")
            except Exception as e:
                logger.warning(f"[connect] Failed to close previous socket for {user_id}")

        await ws.accept()
        self.active.setdefault(user_id, []).append(ws)

    def disconnect(self, user_id: str, ws: WebSocket):
        conns = self.active.get(user_id, [])
        if ws in conns:
            conns.remove(ws)
        if not conns:
            self.active.pop(user_id, None)

    async def notify_user(self, user_id: str, message: dict):
        """Send a JSON message to every WS for that user"""
        logger.info(f"Sending WS to {user_id}: {message}")
        payload = jsonable_encoder(message)
        for ws in list(self.active.get(user_id, [])):
            try:
                await ws.send_json(payload)
            except (WebSocketDisconnect, RuntimeError) as e:
                logger.info(f"[notify_user] socket closed for {user_id}: {e!r}, removing it")
                self.disconnect(user_id, ws)
            except Exception:
                logger.exception(f"[notify_user] unexpected error for {user_id}, removing socket")
                self.disconnect(user_id, ws)


# make a single shared instance
manager = ConnectionManager()