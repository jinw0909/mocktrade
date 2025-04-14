from fastapi import FastAPI
from starlette.responses import JSONResponse
# from routers import ticker, que_chart, candle, analysis
# from models.models import IntervalModel
from routers import trei_routers, settings_routers,frontapi_routers
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
# from utils.ticker_func import TickerList

app = FastAPI()

origins = [ ]

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
    uvicorn.run(app="main:app", host="0.0.0.0", port=8000,reload=True)