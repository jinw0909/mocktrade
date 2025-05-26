from fastapi import APIRouter
from starlette.responses import JSONResponse
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from utils.connections import MySQLAdapter
from fastapi import APIRouter, HTTPException, UploadFile, File
from typing import Optional
from utils.connections import MySQLAdapter
import traceback
from services.trading import TradingService
from services.settings import SettingsService
from services.liquidation import LiquidationService
import logging
router = APIRouter()

logger = logging.getLogger('uvicorn')

MAINT_RATE = 0.005         # first notional tier: 0.5 %

trader = TradingService()
mysql = MySQLAdapter()
settings = SettingsService()
liquidation = LiquidationService()


def calc_iso_liq_price(entry_price: float,
                       leverage: float,
                       side: str) -> float | None:

    if side == 'buy':     # LONG
        return entry_price * (1 - 1/leverage)
    else:                 # SHORT
        return entry_price * (1 + 1/leverage)


@router.get('/settleLimitOrders', summary='settle limit orders', tags=['EXECUTE API'])
async def api_settleLimitOrders():
    count = await trader.settle_limit_orders()
    return {"settled orders": count}


@router.get('/settleTpslOrders', summary='settle tpsl orders', tags=['EXECUTE API'])
async def api_settleTpslOrders():
    count = await trader.settle_tpsl_orders()
    return { "settled orders": count }


@router.post('/close', summary='close existing position', tags=["EXECUTE API"])
async def api_closePosition(user_id: int, symbol: str):
    try:
        query_result = await settings.close_position(user_id, symbol)
        return query_result
    except Exception as e:
        print(str(e))
        return {"error": f"Failed to close the existing position {str(e)}"}


@router.post('/liquidate', summary='liquidate position where condition met', tags=['EXECUTE API'])
async def api_liquidatePositions():
    try:
        count = await liquidation.liquidate_positions()
        return { "number of liquidated positions": count }
    except Exception as e:
        print(str(e))
        traceback.print_exc()
        return {"error": f"Error while liquidating positions: {str(e)}"}


@router.post('/liquidateCross', summary="liquidate cross positions if condition met", tags=["EXECUTE API"])
async def api_liquidateCross():
    try:
        result = await liquidation.liquidate_cross_positions()
        liq_count = result.get('liq_count')
        row_count = result.get('row_count')
        logger.info(f"number of positions liquidated: {liq_count}, number of rows updated with liq_price: {row_count}")
        return {"number of liquidated cross positions": liq_count, "number of rows updated with liq_price" : row_count }
    except Exception:
        logger.exception("Error while liquidating cross positions")


@router.post('/calculate_upnl', summary='calculate unrealized pnl of active positions', tags=['EXECUTE API'])
async def api_calculateUpnl():
    try:
        count = await liquidation.calculate_unrealized_pnl()
        return {"number of unrealized pnl derived": count }
    except Exception as e:
        print(str(e))
        traceback.print_exc()
        return { "error": f"Error while calculating unrealized pnl" }
