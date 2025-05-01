from fastapi import WebSocket
from typing import Dict, List
from fastapi.encoders import jsonable_encoder

class ConnectionManager:
    def __init__(self):
        # user_id -> list of WebSocket objects
        self.active: Dict[str, List[WebSocket]] = {}

    async def connect(self, user_id:str, ws: WebSocket):
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
        payload = jsonable_encoder(message)
        for ws in self.active.get(user_id, []):
            await ws.send_json(payload)

# make a single shared instance
manager = ConnectionManager()