# scheduler.py
import traceback
from datetime import datetime
import requests

from pytz import timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from utils.settings import MySQLAdapter  # adjust if your adapter lives elsewhere
from services.trading import TradingService

from utils.price_cache import prices as price_cache

# CoinGecko simple price endpoint
API_ENDPOINT = "https://api.coingecko.com/api/v3/simple/price"
TZ = timezone("Asia/Seoul")

# map your symbols → CoinGecko IDs
SYMBOL_TO_COINGECKO_ID = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "XRP": "ripple",
}

mysql = MySQLAdapter()
trader = TradingService()


def run_all_jobs():
    update_all_prices()
    liquidate_positions()
    settle_limit_orders()
    settle_tpsl_orders()
    calculate_upnl()


def fetch_prices(symbols):
    rd = mysql._get_redis()
    #new_price = rd.get(f'price:{symbol}USDT')
    """
    Batch‐fetch USD prices for a list of symbols via CoinGecko.
    Returns a dict: { "BTC": 83800.12, "ETH": 1850.34, ... }
    """
    # build a comma‐separated list of IDs
    ids = ",".join(SYMBOL_TO_COINGECKO_ID[s] for s in symbols)
    resp = requests.get(
        API_ENDPOINT,
        params={
            "ids": ids,
            "vs_currencies": "usd"
        },
        timeout=10
    )
    resp.raise_for_status()
    data = resp.json()
    # invert back to symbol → price
    return {
        sym: float(data[SYMBOL_TO_COINGECKO_ID[sym]]["usd"])
        for sym in symbols
        if SYMBOL_TO_COINGECKO_ID[sym] in data
    }


def fetch_prices_from_redis(symbols):
    """
    Pull the latest price for each symbol from Redis.
    Expects your Redis keys to be named like 'price:BTCUSDT', 'price:ETHUSDT', etc.
    Returns a dict: { "BTC": 83800.12, "ETH": 1850.34, ... }
    """
    # get a Redis client
    rd = mysql._get_redis()

    # build the list of Redis keys
    keys = [f"price:{sym}USDT" for sym in symbols]

    # mget all values in one round-trip
    raw_values = rd.mget(keys)  # returns list of bytes or None

    # zip them back into a dict, skipping missing keys
    price_dict = {}
    for sym, raw in zip(symbols, raw_values):
        if raw is None:
            # no value in Redis for this symbol
            # print(f"[WARN] no redis price for {sym}")
            continue

        # decode & parse
        try:
            price_dict[sym] = float(raw)
        except ValueError:
            print(f"[ERR] bad format for {sym}: {raw!r}")

    return price_dict


def update_all_prices():
    mysql = MySQLAdapter()
    conn = mysql._get_connection()
    cursor = conn.cursor()

    try:
        # 1) load symbols from your table
        cursor.execute("SELECT symbol FROM mocktrade.prices")
        symbols = [row["symbol"] for row in cursor.fetchall()]

        if not symbols:
            print(f"[{datetime.now(TZ)}] no symbols found to update")
            return

        # 2) fetch in one go
        new_prices = fetch_prices_from_redis(symbols)

        # overwrite the in-memory cache
        price_cache.clear()
        price_cache.update(new_prices)

        # 3) write them back (optional)
        # for sym, price in new_prices.items():
        #     cursor.execute(
        #         """
        #         UPDATE prices
        #            SET price     = %s,
        #                updatedAt = %s
        #          WHERE symbol    = %s
        #         """,
        #         (price, datetime.now(TZ), sym)
        #     )
        #
        # conn.commit()
        # print(f"[{datetime.now(TZ)}] updated prices for: {', '.join(new_prices)}")
        print(f"updated {len(new_prices)} number of prices from redis")

    except Exception as e:
        conn.rollback()
        traceback.print_exc()
        print(f"[{datetime.now(TZ)}] failed to update prices:", e)

    finally:
        conn.close()


def calculate_upnl():
    try:
        count = mysql.calculate_unrealized_pnl()
        print(
            f'executing calculate_upnl at {datetime.now(timezone("Asia/Seoul"))}. Total {count} number of upnl derived')
    except Exception:
        traceback.print_exc()


def liquidate_positions():
    try:
        count = mysql.liquidate_positions()
        print(
            f"executing liquidate positions at {datetime.now(timezone('Asia/Seoul'))}. Total {count} number of positions liquidated")
    except Exception:
        traceback.print_exc()


def settle_limit_orders():
    try:
        count = trader.settle_limit_orders()
        print(
            f"executing settle_limit_orders at {datetime.now(timezone('Asia/Seoul'))}. Total {count} limit orders settled")
    except Exception:
        traceback.print_exc()


def settle_tpsl_orders():
    try:
        count = trader.settle_tpsl_orders()
        print(
            f"executing settle_tpsl_orders at {datetime.now(timezone('Asia/Seoul'))}. Total {count} tp/sl orders settled")
    except Exception:
        traceback.print_exc()


# ————————————————
# scheduler wiring
# ————————————————
scheduler = AsyncIOScheduler(timezone=TZ)
scheduler.add_job(
    run_all_jobs,
    trigger=IntervalTrigger(minutes=1),
    id="orchestrator",
    replace_existing=True
)


def start_scheduler():
    """Call this on FastAPI startup."""
    # update_all_prices()   # run once immediately
    run_all_jobs()
    # liquidate_positions()
    # settle_limit_orders()
    # settle_tpsl_orders()
    # calculate_upnl()
    scheduler.start()


def shutdown_scheduler():
    """Call this on FastAPI shutdown."""
    try:
        scheduler.shutdown(wait=False)
    except AttributeError:
        pass
