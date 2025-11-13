# scheduler.py
import asyncio
import shutil
import traceback
from datetime import datetime, timedelta
import requests
import logging
import gzip
import os
import fcntl

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

import tempfile, gzip, shutil
import boto3
from botocore.config import Config as BotoConfig
from boto3.s3.transfer import TransferConfig



config = Config(".env")
logger = logging.getLogger(__name__)
logging.getLogger("apscheduler.executors.default").setLevel(logging.WARNING)

# 락 관련 전역 변수들 추가
_LOCK_PATH = "/tmp/mocktrade_scheduler.lock"  # 앱 전용 락 파일 경로
_lock_fd = None
_scheduler_started = False

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

session = boto3.Session()  # will honor env vars & ~/.aws/credentials
sts = session.client("sts")
print("CallerIdentity:", sts.get_caller_identity())

s3 = session.client("s3", config=BotoConfig(retries={"max_attempts": 3, "mode": "standard"}))
print("S3 region (client config):", s3.meta.config.region_name)

import boto3, botocore
bucket = "mocktrade-dumps"
key = "local/mysqldump_202508291103.sql.gz"

s3 = boto3.client("s3")
try:
    head = s3.head_object(Bucket=bucket, Key=key)
    print("FOUND:", head["ContentLength"], "bytes")
except botocore.exceptions.ClientError as e:
    print("HEAD failed:", e.response["Error"]["Code"], e.response["Error"].get("Message"))



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

# async def daily_mysql_dump_s3():
#     tmp_path = None
#     try:
#         # ---- config
#         date_str = datetime.now(TZ).strftime("%Y%m%d%H%M")
#         bucket   = config.get("S3_BUCKET", default="your-bucket-name")
#         prefix   = config.get("S3_PREFIX", default="db-backups/mocktrade").strip().strip("/")
#         s3_key   = f"{prefix}/mysqldump_{date_str}.sql.gz"
#
#         mysqldump = shutil.which(config.get("MYSQLDUMP_BIN", default="mysqldump")) or "mysqldump"
#         aws_bin   = shutil.which("aws") or "/usr/bin/aws"  # adjust for macOS: /opt/homebrew/bin/aws
#
#         if not shutil.which(mysqldump):
#             raise RuntimeError(f"mysqldump not found at {mysqldump}")
#         if not shutil.which(aws_bin):
#             raise RuntimeError(f"AWS CLI not found at {aws_bin}")
#
#         dump_cmd = [
#             mysqldump,
#             f"--host={config.get('HOST')}",
#             f"--user={config.get('USER1')}",
#             f"--password={config.get('PASS')}",
#             "--default-character-set=utf8mb4",
#             "--single-transaction", "--quick",
#             "--routines", "--triggers", "--events",
#             "--set-gtid-purged=OFF",
#             config.get("DBNAME"),
#         ]
#         logger.info(f"[dump] starting mysqldump for DB={config.get('DBNAME')} -> s3://{bucket}/{s3_key}")
#
#         # ---- run mysqldump
#         p_dump = await asyncio.create_subprocess_exec(
#             *dump_cmd,
#             stdout=asyncio.subprocess.PIPE,
#             stderr=asyncio.subprocess.PIPE,
#         )
#
#         # drain stderr concurrently to avoid buffer buildup
#         async def _drain_stderr(proc):
#             return await proc.stderr.read()
#         stderr_task = asyncio.create_task(_drain_stderr(p_dump))
#
#         # ---- write gzip to a temp file
#         with tempfile.NamedTemporaryFile(delete=False, suffix=".sql.gz") as tmp:
#             tmp_path = Path(tmp.name)  # remember path, close handle immediately
#         bytes_written = 0
#         # compresslevel=6 is a good default (speed/size)
#         with gzip.open(tmp_path, "wb", compresslevel=6) as gz:
#             while True:
#                 chunk = await p_dump.stdout.read(1024 * 1024)  # 1MB
#                 if not chunk:
#                     break
#                 gz.write(chunk)
#                 bytes_written += len(chunk)
#
#         # ensure dump finished
#         rc = await p_dump.wait()
#         dump_err = await stderr_task
#         if rc != 0:
#             raise RuntimeError(f"mysqldump failed ({rc}): {dump_err.decode(errors='ignore')}")
#
#         size_mb = os.path.getsize(tmp_path) / (1024 * 1024)
#         logger.info(f"[dump] temp gzip written: {tmp_path} ({size_mb:.2f} MB uncompressed_in≈{bytes_written/1024/1024:.2f} MB)")
#
#         # ---- upload to S3
#         aws_cmd = [
#             aws_bin, "s3", "cp", str(tmp_path), f"s3://{bucket}/{s3_key}",
#             "--content-type", "application/gzip",
#             "--storage-class", config.get("S3_STORAGE_CLASS", default="STANDARD_IA"),
#             "--sse", config.get("S3_SSE", default="AES256"),
#             "--no-progress",
#         ]
#         p_aws = await asyncio.create_subprocess_exec(
#             *aws_cmd,
#             stdout=asyncio.subprocess.PIPE,
#             stderr=asyncio.subprocess.PIPE,
#         )
#         out, err = await p_aws.communicate()
#         if p_aws.returncode != 0:
#             raise RuntimeError(f"aws s3 cp failed ({p_aws.returncode}): {err.decode(errors='ignore')}")
#
#         logger.info(f"[dump] uploaded to s3://{bucket}/{s3_key}")
#
#     except Exception:
#         logger.exception("daily_mysql_dump_s3 failed")
#     finally:
#         # always remove the temp file
#         try:
#             if tmp_path and tmp_path.exists():
#                 tmp_path.unlink()
#                 logger.info(f"[dump] temp file removed: {tmp_path}")
#         except Exception as e:
#             logger.warning(f"[dump] failed to remove temp file {tmp_path}: {e}")

async def daily_mysql_dump_s3():
    tmp_path = None
    try:
        # ---- config
        date_str = datetime.now(TZ).strftime("%Y%m%d%H%M")
        bucket   = config.get("S3_BUCKET", default="your-bucket-name")
        prefix   = config.get("S3_PREFIX", default="db-backups/mocktrade").strip().strip("/")
        s3_key   = f"{prefix}/mysqldump_{date_str}.sql.gz"

        mysqldump = shutil.which(config.get("MYSQLDUMP_BIN", default="mysqldump")) or "mysqldump"
        if not shutil.which(mysqldump):
            raise RuntimeError(f"mysqldump not found at {mysqldump}")

        dump_cmd = [
            mysqldump,
            f"--host={config.get('HOST')}",
            f"--user={config.get('USER1')}",
            f"--password={config.get('PASS')}",
            "--default-character-set=utf8mb4",
            "--single-transaction", "--quick",
            "--routines", "--triggers", "--events",
            "--set-gtid-purged=OFF",
            config.get("DBNAME"),
        ]
        logger.info(f"[dump] starting mysqldump for DB={config.get('DBNAME')} -> s3://{bucket}/{s3_key}")

        # ---- run mysqldump
        p_dump = await asyncio.create_subprocess_exec(
            *dump_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # drain stderr concurrently to avoid buffer buildup
        async def _drain_stderr(proc):
            return await proc.stderr.read()
        stderr_task = asyncio.create_task(_drain_stderr(p_dump))

        # ---- write gzip to a temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".sql.gz") as tmp:
            tmp_path = Path(tmp.name)
        bytes_written = 0

        # compresslevel=6 is a good default (speed/size)
        with gzip.open(tmp_path, "wb", compresslevel=6) as gz:
            while True:
                chunk = await p_dump.stdout.read(1024 * 1024)  # 1MB
                if not chunk:
                    break
                gz.write(chunk)
                bytes_written += len(chunk)

        # ensure dump finished
        rc = await p_dump.wait()
        dump_err = await stderr_task
        if rc != 0:
            raise RuntimeError(f"mysqldump failed ({rc}): {dump_err.decode(errors='ignore')}")

        size_mb = os.path.getsize(tmp_path) / (1024 * 1024)
        logger.info(f"[dump] temp gzip written: {tmp_path} ({size_mb:.2f} MB uncompressed_in≈{bytes_written/1024/1024:.2f} MB)")

        # ---- boto3 client with sane retries
        boto_cfg = BotoConfig(
            retries={"max_attempts": 8, "mode": "adaptive"},
            # Optionally set region if you want to force it:
            # region_name=config.get("AWS_REGION", default=None),
        )
        s3 = boto3.client("s3", config=boto_cfg)

        # multipart transfer config (tune thresholds if needed)
        transfer_cfg = TransferConfig(
            multipart_threshold=64 * 1024 * 1024,  # 64MB
            multipart_chunksize=16 * 1024 * 1024,  # 16MB
            max_concurrency=10,  # parallel parts
            use_threads=True,
        )

        # Extra headers
        sse_algo = config.get("S3_SSE", default="AES256")  # "AES256" or "aws:kms"
        extra_args = {
            "ContentType": "application/gzip",
            # "ContentEncoding": "gzip",
            "StorageClass": config.get("S3_STORAGE_CLASS", default="STANDARD_IA"),
            "ServerSideEncryption": sse_algo,
            "ContentDisposition": f'attachment; filename="{Path(s3_key).name}"',
        }
        if sse_algo == "aws:kms":
            kms_key_id = config.get("S3_KMS_KEY_ID", default=None)
            if kms_key_id:
                extra_args["SSEKMSKeyId"] = kms_key_id

        # ---- upload (boto3 is sync; offload to thread to avoid blocking event loop)
        def _upload():
            # use upload_file for resumable multipart & retries
            s3.upload_file(
                Filename=str(tmp_path),
                Bucket=bucket,
                Key=s3_key,
                ExtraArgs=extra_args,
                Config=transfer_cfg,
            )

        await asyncio.to_thread(_upload)

        logger.info(f"[dump] uploaded to s3://{bucket}/{s3_key}")

    except Exception:
        logger.exception("daily_mysql_dump_s3 failed")
    finally:
        # always remove the temp file
        try:
            if tmp_path and tmp_path.exists():
                tmp_path.unlink()
                logger.info(f"[dump] temp file removed: {tmp_path}")
        except Exception as e:
            logger.warning(f"[dump] failed to remove temp file {tmp_path}: {e}")

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
    next_run_time=datetime.now(TZ),
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
schedule_dump = config.get("SCHEDULE_DUMP", default="false").lower() == "true"
if schedule_dump:
    scheduler.add_job(
        # daily_mysql_dump_simple,
        daily_mysql_dump_s3,
        trigger=CronTrigger(hour=3, minute=15, timezone='Asia/Seoul'),
        next_run_time=datetime.now(),
        id='mysqlDailyDump',
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=24*3600,
    )


def _acquire_process_lock() -> bool:
    """
    여러 gunicorn worker 중에서 딱 1개만 락을 잡도록 하는 함수.
    락을 잡은 프로세스만 scheduler를 시작한다.
    """
    global _lock_fd
    try:
        fd = os.open(_LOCK_PATH, os.O_CREAT | os.O_RDWR)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)  # 논블로킹 락
        _lock_fd = fd
        logger.info("Scheduler lock acquired in PID %s", os.getpid())
        return True
    except BlockingIOError:
        # 이미 다른 프로세스가 락을 가지고 있음
        logger.info("Scheduler lock already held by another process. PID %s skips.", os.getpid())
        return False
    except Exception as e:
        logger.exception("Failed to acquire scheduler lock: %s", e)
        return False

#
# def start_scheduler():
#     """Call this on FastAPI startup."""
#     scheduler.start()

def start_scheduler():
    """Call this on FastAPI startup."""
    global _scheduler_started

    if _scheduler_started:
        logger.info("Scheduler already started in this process, skipping.")
        return

    # 🔒 먼저 락을 시도해서 '리더' 프로세스만 스케줄러 실행
    if not _acquire_process_lock():
        # 다른 worker가 이미 스케줄러를 돌리고 있음
        return

    logger.info("Starting APScheduler in PID %s", os.getpid())
    scheduler.start()
    _scheduler_started = True

#
# def shutdown_scheduler():
#     """Call this on FastAPI shutdown."""
#     try:
#         scheduler.shutdown(wait=False)
#     except AttributeError:
#         pass

def shutdown_scheduler():
    """Call this on FastAPI shutdown."""
    global _scheduler_started, _lock_fd

    if _scheduler_started:
        logger.info("Shutting down APScheduler in PID %s", os.getpid())
        try:
            scheduler.shutdown(wait=False)
        except Exception as e:
            logger.warning("Error during scheduler shutdown: %s", e)
        _scheduler_started = False

    # 락 해제
    if _lock_fd is not None:
        try:
            fcntl.flock(_lock_fd, fcntl.LOCK_UN)
            os.close(_lock_fd)
            logger.info("Scheduler lock released in PID %s", os.getpid())
        except OSError:
            pass
        _lock_fd = None
