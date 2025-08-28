# scheduler.py
import asyncio
import traceback
from datetime import datetime, timedelta
import requests
import logging
import gzip

from pytz import timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from services.settings import SettingsService
from utils.connections import MySQLAdapter  # adjust if your adapter lives elsewhere
from services.trading import TradingService
from utils.local_redis import (update_position_status_to_redis,
                               update_balance_status_to_redis,
                               update_order_status_to_redis,
                               update_liq_price)
from services.calculation import CalculationService
# from services.realtime import RealtimeService
from starlette.config import Config
from pathlib import Path
from datetime import datetime

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
# realtime = RealtimeService()
svc = SettingsService()

# if not BACKUP_DIR.is_absolute():
#     BACKUP_DIR = (BASE_DIR / BACKUP_DIR).resolve()


# async def daily_mysql_dump_simple():
#     """
#     Simple daily mysqldump using env vars from `config`.
#     Creates gzip file: /backups/mysql/mysqldump_YYYY-MM-DD.sql.gz
#     """
#     try:
#         BACKUP_DIR.mkdir(parents=True, exist_ok=True)
#
#         date_str = datetime.now(TZ).strftime("%Y-%m-%d")
#         out_file = BACKUP_DIR / f"mysqldump_{date_str}.sql.gz"
#
#         dump_cmd = [
#             "mysqldump",
#             f"--host={config.get('HOST')}",
#             f"--user={config.get('USER1')}",
#             f"--password={config.get('PASS')}",   # note the '=' to avoid prompt
#             "--default-character-set=utf8mb4",
#             "--single-transaction",
#             "--quick",
#             "--routines",
#             "--triggers",
#             "--events",
#             "--set-gtid-purged=OFF",
#             "--databases", config.get("DBNAME"),
#         ]
#
#         logger.info(f"[dump] starting mysqldump for DB={config.get('DBNAME')} -> {out_file}")
#
#         # mysqldump | gzip > file
#         p_dump = await asyncio.create_subprocess_exec(
#             *dump_cmd,
#             stdout=asyncio.subprocess.PIPE,
#             stderr=asyncio.subprocess.PIPE,
#         )
#
#         with gzip.open(out_file, "wb") as gz:
#             while True:
#                 chunk = await p_dump.stdout.read(1024 * 1024)
#                 if not chunk:
#                     break
#                 gz.write(chunk)
#
#         _, dump_err = await p_dump.communicate()
#
#         if p_dump.returncode != 0:
#             out_file.unlink(missing_ok=True)
#             raise RuntimeError(f"[dump] mysqldump failed ({p_dump.returncode}): {dump_err.decode(errors='ignore')}")
#
#         logger.info(f"[dump] wrote {out_file} ({out_file.stat().st_size/1024/1024:.2f} MB)")
#     except Exception:
#         logger.exception("daily_mysql_dump_simple failed")

BASE_DIR = Path(__file__).resolve().parent
BACKUP_DIR = Path(config.get("MYSQL_BACKUP_DIR", default="./backups/mysql")).expanduser()

async def daily_mysql_dump_simple():
    """
    Stream mysqldump -> gzip file. Creds from env via `config`.
    """
    try:
        # Work with a local variable
        backup_dir = BACKUP_DIR
        if not backup_dir.is_absolute():
            backup_dir = (BASE_DIR / backup_dir).resolve()

        backup_dir.mkdir(parents=True, exist_ok=True)

        date_str = datetime.now(TZ).strftime("%Y%m%d%H%M")
        out_file = backup_dir / f"mysqldump_{date_str}.sql.gz"

        dump_cmd = [
            "mysqldump",
            f"--host={config.get('HOST')}",
            f"--user={config.get('USER1')}",
            f"--password={config.get('PASS')}",   # '=' prevents interactive prompt
            "--default-character-set=utf8mb4",
            "--single-transaction",
            "--quick",
            "--routines", "--triggers", "--events",
            "--set-gtid-purged=OFF",
            config.get("DBNAME"),
        ]

        logger.info(f"[dump] starting mysqldump for DB={config.get('DBNAME')} -> {out_file}")

        p_dump = await asyncio.create_subprocess_exec(
            *dump_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # Drain stderr concurrently while streaming stdout -> gzip
        stderr_task = asyncio.create_task(p_dump.stderr.read())

        with gzip.open(out_file, "wb") as gz:
            while True:
                chunk = await p_dump.stdout.read(1024 * 1024)  # 1MB
                if not chunk:
                    break
                gz.write(chunk)

        # Wait for process to finish and collect stderr
        returncode = await p_dump.wait()
        dump_err = await stderr_task

        if returncode != 0:
            out_file.unlink(missing_ok=True)
            raise RuntimeError(f"[dump] mysqldump failed ({returncode}): {dump_err.decode(errors='ignore')}")

        logger.info(f"[dump] wrote {out_file} ({out_file.stat().st_size/1024/1024:.2f} MB)")

        # Retention policy
        cutoff = datetime.now(TZ) - timedelta(days=30)
        removed = 0
        for f in backup_dir.glob("mysqldump_*.sql.gz"):
            try:
                mtime = datetime.fromtimestamp(f.stat().st_mtime, TZ)
                if mtime < cutoff:
                    f.unlink(missing_ok=True)
                    removed += 1
            except Exception as e:
                logger.warning(f"[dump] failed to remove {f}: {e}")

        if removed:
            logger.info(f"[dump] removed {removed} old backups")

    except Exception:
        logger.exception("daily_mysql_dump_simple failed")

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
scheduler.add_job(
    svc.reload_symbol_cache,
    trigger=IntervalTrigger(days=1),
    id="precisionCacheUpdater",
    next_run_time=datetime.now(),
    replace_existing=True
)
scheduler.add_job(
    svc.update_precision,
    trigger=IntervalTrigger(days=1),
    id="precisionUpdater",
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
scheduler.add_job(
    daily_mysql_dump_simple,
    trigger=CronTrigger(hour=3, minute=15, timezone='Asia/Seoul'),
    next_run_time=datetime.now(),
    id='mysqlDailyDump',
    replace_existing=True,
    max_instances=1,
    coalesce=True,
    misfire_grace_time=3600,
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


