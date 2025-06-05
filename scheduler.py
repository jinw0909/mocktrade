# scheduler.py
import traceback
from datetime import datetime
import requests
import logging

from pytz import timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from utils.connections import MySQLAdapter  # adjust if your adapter lives elsewhere
from services.trading import TradingService
from utils.local_redis import (update_position_status_to_redis,
                               update_balance_status_to_redis,
                               update_order_status_to_redis,
                               update_liq_price)
from services.calculation import CalculationService
from services.realtime import RealtimeService
from starlette.config import Config

config = Config(".env")
logger = logging.getLogger(__name__)
logging.getLogger("apscheduler.executors.default").setLevel(logging.WARNING)
# CoinGecko simple price endpoint
API_ENDPOINT = "https://api.coingecko.com/api/v3/simple/price"
TZ = timezone("Asia/Seoul")

# map your symbols → CoinGecko IDs
SYMBOL_TO_COINGECKO_ID = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "XRP": "ripple",
}
# mysql = MySQLAdapter()
trader = TradingService()
calculation = CalculationService()
realtime = RealtimeService()

async def calculate_cross():
    try:
        result = await trader.calculate_cross_positions()
        row_count = result.get('row_count', 0)
        logger.info(
            f"calculating cross position liquidation price at {datetime.now(timezone('Asia/Seoul'))}. Total {row_count} liquidation price derived")
    except Exception:
        logger.exception("Error during calculating cross position liquidation price")


async def liquidate_cross():
    try:
        liquidated_positions = await trader.liquidate_cross_positions()
        user_and_position = [{'user_id': lp['user_id'], 'position_id': lp['position_id']} for lp in
                             liquidated_positions]
        logger.info(
            f"executing liquidate_cross_position at {datetime.now(timezone('Asia/Seoul'))}. Total {len(liquidated_positions)} positions liquidated")
        logger.info(user_and_position)
    except Exception:
        logger.exception("Error during calculating cross margin positions")


async def calculate_upnl():
    try:
        count = await trader.calculate_unrealized_pnl()
        # print(f'executing calculate_upnl at {datetime.now(timezone("Asia/Seoul"))}. Total {count} number of upnl derived')
        logger.info(
            f'executing calculate_upnl at {datetime.now(timezone("Asia/Seoul"))}. Total {count} number of upnl derived')
    except Exception:
        # traceback.print_exc()
        logger.exception("Failed to update prices:")


async def liquidate_positions():
    try:
        count = await trader.liquidate_positions()
        # print(f"executing liquidate positions at {datetime.now(timezone('Asia/Seoul'))}. Total {count} number of positions liquidated")
        logger.info(
            f"executing liquidate positions at {datetime.now(timezone('Asia/Seoul'))}. Total {count} number of positions liquidated")
    except Exception:
        # traceback.print_exc()
        logger.exception("Failed to update prices:")


async def settle_limit_orders():
    try:
        count = await trader.settle_limit_orders()
        # print(f"executing settle_limit_orders at {datetime.now(timezone('Asia/Seoul'))}. Total {count} limit orders settled")
        logger.info(
            f"executing settle_limit_orders at {datetime.now(timezone('Asia/Seoul'))}. Total {count} limit orders settled")
    except Exception:
        # traceback.print_exc()
        logger.exception("Failed to update prices:")


async def settle_tpsl_orders():
    try:
        count = await trader.settle_tpsl_orders()
        # print(f"executing settle_tpsl_orders at {datetime.now(timezone('Asia/Seoul'))}. Total {count} tp/sl orders settled")
        logger.info(
            f"executing settle_tpsl_orders at {datetime.now(timezone('Asia/Seoul'))}. Total {count} tp/sl orders settled")

    except Exception:
        # traceback.print_exc()
        logger.exception("Failed to settle tp/sl orders")


async def update_status_to_redis():
    try:
        logger.info(f"Start updating MySQL status to redis at {datetime.now(timezone('Asia/Seoul'))}")
        await update_position_status_to_redis()
        await update_order_status_to_redis()
        await update_balance_status_to_redis()
        await update_liq_price()
    except Exception:
        logger.exception("Failed to update MySQL status to Redis")

# ————————————————
# scheduler wiring
# ————————————————
scheduler = AsyncIOScheduler(timezone=TZ)

interval_sec = int(config.get('STATUS_INTERVAL'))
scheduler.add_job(
    update_status_to_redis,
    trigger=IntervalTrigger(seconds=interval_sec),
    next_run_time=datetime.now(),
    id="statusUpdater",
    replace_existing=True
)
pnl_sec = int(config.get('PNL_INTERVAL'))
scheduler.add_job(
    calculation.calculate_pnl,
    trigger=IntervalTrigger(seconds=pnl_sec),
    # next_run_time=datetime.now(),
    id="pnlCalculator",
    replace_existing=True
)
liq_sec = int(config.get('LIQ_INTERVAL'))
scheduler.add_job(
    calculation.calculate_liq_prices,
    trigger=IntervalTrigger(seconds=liq_sec),
    # next_run_time=datetime.now(),
    id="liqCalculator",
    replace_existing=True
)
limit_sec = int(config.get('LIMIT_INTERVAL'))
scheduler.add_job(
    # calculation.settle_orders,
    calculation.settle_orders,
    trigger=IntervalTrigger(seconds=limit_sec),
    # next_run_time=datetime.now(),
    id="orderSettler",
    replace_existing=True
)


def start_scheduler():
    """Call this on FastAPI startup."""
    scheduler.start()


def shutdown_scheduler():
    """Call this on FastAPI shutdown."""
    try:
        scheduler.shutdown(wait=False)
    except AttributeError:
        pass
#
# if __name__ == "__main__":
#     import asyncio
#     import logging
#
#     logging.basicConfig(level=logging.INFO)
#     logger.info("Starting standalone APScheduler service...")
#
#     async def start():
#         start_scheduler()
#
#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
#     loop.run_until_complete(start())
#     loop.run_forever()


