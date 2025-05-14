import uvicorn
from fastapi import FastAPI
from fastapi import Request
from urllib.parse import parse_qs
from starlette.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from routers import trei_routers, settings_routers, frontapi_routers, auth_routers

# FastAPI 애플리케이션 설정
app = FastAPI()


origins = [
    "http://localhost:5173", 
    "http://127.0.0.1:8000",
    'http://127.0.0.1:5173',
    'http://localhost:8000',
    'http://localhost:8080',
    "http://127.0.0.1:8080",
    'http://172.30.1.80:8080',
    'https://hjzheld.github.io',
    'https://dominance.retri.xyz',
    'https://dominance.tryex.xyz',
    'https://tryex.xyz'
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(trei_routers.router, prefix='/trading')
app.include_router(frontapi_routers.router, prefix='/user')
app.include_router(settings_routers.router, prefix='/settings')
app.include_router(auth_routers.router, prefix='/session')

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)