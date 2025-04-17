from fastapi import FastAPI
from starlette.responses import JSONResponse
# from routers import ticker, que_chart, candle, analysis
# from models.models import IntervalModel
from routers import trei_routers, settings_routers, frontapi_routers, execute_routers
from fastapi.middleware.cors import CORSMiddleware

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
    'https://hjzheld.github.io/MainPage/',
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
app.include_router(execute_routers.router, prefix='/execute')
app.include_router(auth_routers.router, prefix='/session')
# @app.get('/list', tags=['코인 리스트'], summary='코인 항목')
# async def ticker_list():
#     try:
#         client = TickerList()
#         client.get_ticker_list()

#         if client.send_text:
#             pass

#     except Exception as e:
#         pass

#     return JSONResponse(client.return_dict_data, status_code=client.status_code)
    
# #Router 생성
# app.include_router(candle.router, prefix='/candle', tags=['기본 Candle API'])
# app.include_router(analysis.router, prefix='/analysis', tags=['분석 API'])
# app.include_router(que_chart.router, prefix='/que', tags=['Que Chart API'])
# app.include_router(ticker.router, prefix='/ticker', tags=['Ticker API'])



# 메인 화면
@app.get('/', tags=['Main'], summary='메인 화면 200 지정', deprecated=True)
async def root():

    return JSONResponse(dict(tickers='Good'), status_code=200)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)