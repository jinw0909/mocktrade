
import json
import logging
from datetime import datetime

import redis.asyncio as aioredis
from pytz import timezone
from starlette.config import Config

from utils.connections import MySQLAdapter


config = Config(".env")
logger = logging.getLogger("uvicorn")

# API 서버에서 스케줄러 서버의 Redis를 바라봄
redis_client = aioredis.Redis(
    host=config.get("LOCAL_REDIS"),
    port=6379,
    db=0,
    decode_responses=True,
)

# 가격 Redis
price_redis = aioredis.Redis(
    host=config.get("REDIS_HOST"),
    port=6379,
    db=0,
    decode_responses=True,
)

mysql = MySQLAdapter()


def resolve_liq_price(existing_blob, pos_id, db_liq_price):
    """
    Redis에 이미 계산된 청산가가 있고,
    Redis 포지션과 DB 포지션의 pos_id가 같으면 Redis 값을 우선 사용한다.

    동일 심볼 포지션을 종료한 뒤 재진입한 경우에는 pos_id가 달라지므로
    이전 포지션의 청산가를 새 포지션에 물려주지 않는다.
    """

    if existing_blob:
        try:
            existing_data = json.loads(existing_blob)

            existing_pos_id = existing_data.get("pos_id")
            existing_liq_price = existing_data.get("liq_price")

            if (
                    existing_pos_id is not None
                    and str(existing_pos_id) == str(pos_id)
                    and existing_liq_price is not None
            ):
                return float(existing_liq_price)

        except (TypeError, ValueError, json.JSONDecodeError):
            pass

    return float(db_liq_price or 0)


async def update_position_status_to_redis():
    logger.info("Updating MySQL position status to Redis")

    conn = None
    cursor = None

    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                ph.id AS pos_id,
                ph.status AS status,
                ph.user_id,
                ph.symbol,
                ph.size,
                ph.amount,
                ph.entry_price,
                ph.liq_price,
                ph.margin,
                ph.pnl,
                ph.margin_type,
                ph.side,
                ph.leverage,
                ph.tp,
                ph.sl,
                ph.close_price,
                ph.unrealized_pnl,
                ph.unrealized_pnl_pct,
                u.retri_id
            FROM mocktrade.position_history AS ph
                     JOIN mocktrade.user AS u
                          ON ph.user_id = u.id
            WHERE ph.status = 1
              AND ph.amount > 0 
            """
        )

        rows = cursor.fetchall()
        positions_by_user = {}

        for row in rows:
            retri_id = row["retri_id"]
            symbol = row["symbol"]
            redis_key = f"positions:{retri_id}"

            amount = float(row["amount"] or 0)
            entry_price = float(row["entry_price"] or 0)
            side = row["side"]
            leverage = int(row["leverage"]) if row["leverage"] else 1
            margin_type = row["margin_type"]

            price_key = f"price:{symbol}USDT"
            price_raw = await price_redis.get(price_key)

            try:
                current_price = float(price_raw) if price_raw else entry_price
            except (TypeError, ValueError):
                current_price = entry_price

            size = current_price * amount

            margin = float(row["margin"] or 0)

            if margin_type == "cross" and leverage:
                margin = size / leverage

            if side == "buy":
                pnl = (current_price - entry_price) * amount
            else:
                pnl = (entry_price - current_price) * amount

            initial_notional = entry_price * amount

            pnl_pct = (
                (pnl / initial_notional) * 100
                if initial_notional
                else 0.0
            )

            roi_pct = (
                (pnl / margin) * 100
                if margin
                else 0.0
            )

            # 계산된 cross 청산가가 Redis에 있으면 보존
            # 단, 같은 pos_id일 때만 재사용
            existing_blob = await redis_client.hget(redis_key, symbol)

            final_liq_price = resolve_liq_price(
                existing_blob=existing_blob,
                pos_id=row["pos_id"],
                db_liq_price=row["liq_price"],
            )

            positions_by_user.setdefault(retri_id, []).append(
                {
                    "pos_id": row["pos_id"],
                    "user_id": row["user_id"],
                    "symbol": symbol,
                    "entry_price": entry_price,
                    "liq_price": final_liq_price,
                    "market_price": current_price,
                    "amount": amount,
                    "side": side,
                    "margin": margin,
                    "margin_type": margin_type,
                    "size": size,
                    "leverage": leverage,
                    "tp": row["tp"],
                    "sl": row["sl"],
                    "unrealized_pnl": pnl,
                    "unrealized_pnl_pct": pnl_pct,
                    "roi_pct": roi_pct,
                }
            )

        # 사용자별 활성 포지션 Redis 갱신
        for retri_id, position_list in positions_by_user.items():
            redis_key = f"positions:{retri_id}"

            await redis_client.delete(redis_key)

            mapping = {
                position["symbol"]: json.dumps(position)
                for position in position_list
            }

            if mapping:
                await redis_client.hset(redis_key, mapping=mapping)

        # 활성 포지션이 사라진 사용자 Redis 정리
        async for redis_key in redis_client.scan_iter("positions:*"):
            try:
                _, retri_id = redis_key.split(":", 1)
            except ValueError:
                continue

            if retri_id not in positions_by_user:
                await redis_client.delete(redis_key)

        logger.info(
            "Updated positions for %s users at %s",
            len(positions_by_user),
            datetime.now(timezone("Asia/Seoul")),
        )

    except Exception:
        logger.exception("Failed to update position status to Redis")

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()


async def update_position_status_per_user(user_id, retri_id=None):
    conn = None
    cursor = None

    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                ph.id AS pos_id,
                ph.user_id,
                ph.symbol,
                ph.size,
                ph.amount,
                ph.entry_price,
                ph.liq_price,
                ph.margin,
                ph.pnl,
                ph.margin_type,
                ph.side,
                ph.leverage,
                ph.status,
                ph.tp,
                ph.sl,
                ph.close_price,
                ph.unrealized_pnl,
                ph.unrealized_pnl_pct
            FROM mocktrade.position_history AS ph
            WHERE ph.user_id = %s
              AND ph.status = 1
              AND ph.amount > 0  
            """,
            (user_id,),
        )

        position_rows = cursor.fetchall()

        if not retri_id:
            cursor.execute(
                """
                SELECT retri_id
                FROM mocktrade.user
                WHERE id = %s
                  AND status = 0
                LIMIT 1
                """,
                (user_id,),
            )

            user_row = cursor.fetchone()

            if not user_row:
                logger.warning("User %s not found", user_id)
                raise LookupError(
                    f"retri_id of user_id={user_id} not found"
                )

            retri_id = user_row["retri_id"]

        redis_key = f"positions:{retri_id}"
        positions = []

        for row in position_rows:
            symbol = row["symbol"]

            entry_price = float(row["entry_price"] or 0)
            amount = float(row["amount"] or 0)
            side = row["side"]
            leverage = int(row["leverage"]) if row["leverage"] else 1
            margin_type = row["margin_type"]

            price_key = f"price:{symbol}USDT"
            price_raw = await price_redis.get(price_key)

            try:
                current_price = float(price_raw) if price_raw else entry_price
            except (TypeError, ValueError):
                current_price = entry_price

            size = current_price * amount

            margin = float(row["margin"] or 0)

            if margin_type == "cross" and leverage:
                margin = size / leverage

            if side == "buy":
                pnl = (current_price - entry_price) * amount
            else:
                pnl = (entry_price - current_price) * amount

            initial_notional = entry_price * amount

            pnl_pct = (
                (pnl / initial_notional) * 100
                if initial_notional
                else 0.0
            )

            roi_pct = (
                (pnl / margin) * 100
                if margin
                else 0.0
            )

            # 기존 Redis 값은 삭제 전에 먼저 조회
            # 동일한 pos_id의 포지션에 한해서 계산된 청산가를 보존
            existing_blob = await redis_client.hget(redis_key, symbol)

            final_liq_price = resolve_liq_price(
                existing_blob=existing_blob,
                pos_id=row["pos_id"],
                db_liq_price=row["liq_price"],
            )

            positions.append(
                {
                    "pos_id": row["pos_id"],
                    "user_id": row["user_id"],
                    "symbol": symbol,
                    "entry_price": entry_price,
                    "liq_price": final_liq_price,
                    "market_price": current_price,
                    "amount": amount,
                    "side": side,
                    "margin": margin,
                    "margin_type": margin_type,
                    "size": size,
                    "leverage": leverage,
                    "tp": row["tp"],
                    "sl": row["sl"],
                    "unrealized_pnl": pnl,
                    "unrealized_pnl_pct": pnl_pct,
                    "roi_pct": roi_pct,
                }
            )

        await redis_client.delete(redis_key)

        mapping = {
            position["symbol"]: json.dumps(position)
            for position in positions
        }

        if mapping:
            await redis_client.hset(redis_key, mapping=mapping)

        logger.info(
            "Completed updating position status of user [%s] to Redis",
            user_id,
        )

    except Exception:
        logger.exception(
            "Failed to update position status of user [%s] to Redis",
            user_id,
        )

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()


async def update_order_status_to_redis():
    logger.info("Updating MySQL order status to Redis")

    conn = None
    cursor = None

    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                oh.id AS or_id,
                oh.status AS status,
                oh.magin AS margin,
                oh.user_id,
                oh.symbol,
                oh.type,
                oh.margin_type,
                oh.side,
                oh.price,
                oh.amount,
                oh.leverage,
                oh.order_price,
                oh.po_id,
                oh.tp,
                oh.sl,
                u.retri_id
            FROM mocktrade.order_history AS oh
                     JOIN mocktrade.user AS u
                          ON oh.user_id = u.id
            WHERE oh.status = 0
            """
        )

        rows = cursor.fetchall()
        orders_by_user = {}

        for row in rows:
            retri_id = row["retri_id"]

            orders_by_user.setdefault(retri_id, []).append(
                {
                    "or_id": row["or_id"],
                    "user_id": row["user_id"],
                    "symbol": row["symbol"],
                    "price": row["price"],
                    "type": row["type"],
                    "margin_type": row["margin_type"],
                    "margin": row["margin"],
                    "leverage": row["leverage"],
                    "side": row["side"],
                    "order_price": row["order_price"],
                    "amount": row["amount"],
                    "tp": row["tp"],
                    "sl": row["sl"],
                    "po_id": row["po_id"],
                }
            )

        for retri_id, order_list in orders_by_user.items():
            redis_key = f"orders:{retri_id}"
            payload = json.dumps(order_list)

            await redis_client.set(redis_key, payload)

        async for redis_key in redis_client.scan_iter("orders:*"):
            try:
                _, retri_id = redis_key.split(":", 1)
            except ValueError:
                continue

            if retri_id not in orders_by_user:
                await redis_client.delete(redis_key)

        logger.info(
            "Updated orders for %s users at %s",
            len(orders_by_user),
            datetime.now(timezone("Asia/Seoul")),
        )

    except Exception:
        logger.exception("Failed to update order status to Redis")

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()


async def update_order_status_per_user(user_id, retri_id=None):
    conn = None
    cursor = None

    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                oh.id AS or_id,
                oh.status AS status,
                oh.magin AS margin,
                oh.user_id,
                oh.symbol,
                oh.type,
                oh.margin_type,
                oh.side,
                oh.price,
                oh.amount,
                oh.leverage,
                oh.order_price,
                oh.po_id,
                oh.tp,
                oh.sl
            FROM mocktrade.order_history AS oh
            WHERE oh.user_id = %s
              AND oh.status = 0
            """,
            (user_id,),
        )

        order_rows = cursor.fetchall()

        if not retri_id:
            cursor.execute(
                """
                SELECT retri_id
                FROM mocktrade.user
                WHERE id = %s
                  AND status = 0
                LIMIT 1
                """,
                (user_id,),
            )

            user_row = cursor.fetchone()

            if not user_row:
                logger.warning("User %s not found", user_id)
                raise LookupError(
                    f"retri_id of user_id={user_id} not found"
                )

            retri_id = user_row["retri_id"]

        orders = [
            {
                "or_id": row["or_id"],
                "user_id": row["user_id"],
                "symbol": row["symbol"],
                "price": row["price"],
                "type": row["type"],
                "margin_type": row["margin_type"],
                "margin": row["margin"],
                "side": row["side"],
                "leverage": row["leverage"],
                "order_price": row["order_price"],
                "amount": row["amount"],
                "tp": row["tp"],
                "sl": row["sl"],
                "po_id": row["po_id"],
            }
            for row in order_rows
        ]

        redis_key = f"orders:{retri_id}"
        payload = json.dumps(orders)

        await redis_client.set(redis_key, payload)

        logger.info(
            "Completed updating order status of user [%s] to Redis",
            user_id,
        )

    except Exception:
        logger.exception(
            "Failed to update order status of user [%s] to Redis",
            user_id,
        )

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()


async def update_balance_status_to_redis():
    logger.info("Updating MySQL balance status to Redis")

    conn = None
    cursor = None

    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                balance,
                retri_id
            FROM mocktrade.user
            WHERE status = 0
            """
        )

        user_rows = cursor.fetchall()

        balances_by_user = {
            row["retri_id"]: float(row["balance"] or 0)
            for row in user_rows
        }

        # Redis SET은 기존 값을 원자적으로 덮어쓴다.
        # delete 후 set을 하면 그 사이에 balance key가 잠깐 사라질 수 있으므로
        # 절대 delete하지 않는다.
        for retri_id, balance in balances_by_user.items():
            redis_key = f"balances:{retri_id}"
            await redis_client.set(redis_key, balance)

        # DB에 더 이상 존재하지 않는 유저의 stale key만 정리
        async for redis_key in redis_client.scan_iter("balances:*"):
            try:
                _, retri_id = redis_key.split(":", 1)
            except ValueError:
                continue

            if retri_id not in balances_by_user:
                await redis_client.delete(redis_key)
                await redis_client.delete(f"availables:{retri_id}")

        logger.info(
            "Updated balances for %s users at %s",
            len(balances_by_user),
            datetime.now(timezone("Asia/Seoul")),
        )

    except Exception:
        logger.exception("Failed to update balance status to Redis")

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()

async def update_balance_status_per_user(user_id, retri_id=None):
    conn = None
    cursor = None

    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()

        if not user_id:
            cursor.execute(
                """
                SELECT id
                FROM mocktrade.user
                WHERE retri_id = %s
                  AND status = 0
                LIMIT 1
                """,
                (retri_id,),
            )

            user_row = cursor.fetchone()

            if not user_row:
                logger.warning(
                    "There is no user with retri_id [%s]",
                    retri_id,
                )
                return

            user_id = user_row["id"]

        cursor.execute(
            """
            SELECT
                retri_id,
                balance
            FROM mocktrade.user
            WHERE id = %s
              AND status = 0
            """,
            (user_id,),
        )

        user_row = cursor.fetchone()

        if not user_row:
            logger.warning(
                "Could not find user with id [%s]",
                user_id,
            )
            return

        retri_id = user_row["retri_id"]
        balance = float(user_row["balance"] or 0)

        redis_key = f"balances:{retri_id}"

        # delete 없이 바로 원자적으로 덮어쓰기
        await redis_client.set(redis_key, balance)

        logger.info(
            "Completed updating balance status of user [%s] to Redis (%s)",
            user_id,
            balance,
        )

    except Exception:
        logger.exception(
            "Failed to update balance status of user [%s] to Redis",
            user_id,
        )

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()


async def update_liq_price():
    """
    calculation.py가 positions:{retri_id} 해시 내부에 저장한
    cross 포지션 청산가를 MySQL position_history에 업로드한다.
    """

    logger.info("Uploading cross position liquidation prices to MySQL")

    conn = None
    cursor = None
    row_count = 0

    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()

        async for redis_key in redis_client.scan_iter("positions:*"):
            raw_hash = await redis_client.hgetall(redis_key)

            if not raw_hash:
                continue

            for _, blob in raw_hash.items():
                try:
                    position_data = json.loads(blob)
                except (TypeError, json.JSONDecodeError):
                    logger.warning(
                        "Failed to parse position data from Redis key [%s]",
                        redis_key,
                    )
                    continue

                # cross 청산가만 주기적으로 업로드
                if position_data.get("margin_type") != "cross":
                    continue

                pos_id = position_data.get("pos_id")
                liq_price = position_data.get("liq_price")

                if pos_id is None or liq_price is None:
                    continue

                cursor.execute("SAVEPOINT liq_updt")

                try:
                    cursor.execute(
                        """
                        UPDATE mocktrade.position_history
                        SET liq_price = %s
                        WHERE id = %s
                          AND status = 1
                        """,
                        (liq_price, pos_id),
                    )

                    row_count += cursor.rowcount

                    cursor.execute("RELEASE SAVEPOINT liq_updt")

                except Exception as exc:
                    logger.warning(
                        "Failed updating liquidation price for pos_id=%s: %s",
                        pos_id,
                        exc,
                    )

                    cursor.execute("ROLLBACK TO SAVEPOINT liq_updt")
                    cursor.execute("RELEASE SAVEPOINT liq_updt")

        conn.commit()

        logger.info(
            "Updated %s liquidation prices successfully to MySQL",
            row_count,
        )

    except Exception:
        if conn:
            conn.rollback()

        logger.exception(
            "Critical failure while uploading liquidation prices to MySQL"
        )

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()