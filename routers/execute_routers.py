from fastapi import APIRouter
from starlette.responses import JSONResponse
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from utils.settings import MySQLAdapter
from fastapi import APIRouter, HTTPException, UploadFile, File
from typing import Optional
from utils.settings import MySQLAdapter
import traceback
from services.trading import TradingService
router = APIRouter()

MAINT_RATE = 0.005         # first notional tier: 0.5 %

def calculate_binance_liq_price(entry_price, leverage, side):
    if side == "buy":
        return entry_price * (1 - (1 / leverage))
    else:
        return entry_price * (1 + (1 / leverage))


def aggregate_binance_style_position(settled_orders):
    if not settled_orders:
        return None

    symbol = settled_orders[0]["symbol"]
    user_id = settled_orders[0]["user_id"]

    # Separate into long and short
    long_orders = [o for o in settled_orders if o["side"] == "buy"]
    short_orders = [o for o in settled_orders if o["side"] == "sell"]

    long_amount = sum(o["amount"] for o in long_orders)
    short_amount = sum(o["amount"] for o in short_orders)

    net_amount = long_amount - short_amount

    if net_amount == 0:
        return None  # net zero position

    # Determine side and net amount
    side = "buy" if net_amount > 0 else "sell"
    total_amount = abs(net_amount)

    total_size = sum(o["price"] * o["amount"] for o in settled_orders)
    total_margin = sum((o["price"] * o["amount"]) / o["leverage"] for o in settled_orders)

    weighted_entry_price = total_size / sum(o["amount"] for o in settled_orders)
    effective_leverage = total_size / total_margin
    print(
        f"total size: {total_size}, total_margin: {total_margin}, weighted_entry_price: {weighted_entry_price}, effective leverage: {effective_leverage}")

    liq_price = calculate_binance_liq_price(
        entry_price=weighted_entry_price,
        leverage=effective_leverage,
        side=side
    )

    return {
        "user_id": user_id,
        "symbol": symbol,
        "side": side,
        "entry_price": round(weighted_entry_price, 6),
        "amount": round(total_amount, 6),
        "size": round(total_size, 6),
        "margin": round(total_margin, 6),
        "margin_type": 'isolated',
        "leverage": round(effective_leverage, 2),
        "liq_price": round(liq_price, 6)
    }


def calc_released_margin(current_margin, new_margin):
    """Margin that will be credited back to the account."""
    return max(current_margin - new_margin, 0.0)

MAINT_RATE = 0.005   # 0.5 % first tier

def calc_iso_liq_price(entry_price: float,
                       leverage: float,
                       side: str) -> float | None:

    if side == 'buy':     # LONG
        return entry_price * (1 - 1/leverage)
    else:                 # SHORT
        return entry_price * (1 + 1/leverage)


def calculate_position(current_position, order):
    """
    Calculates the resulting position after applying an order (market or filled limit).
    If position flips, calls mysql.position_flip and returns a fresh position.
    """

    user_id = order['user_id']
    symbol = order['symbol']
    side = order['side']  # buy or sell
    amount = float(order['amount'])
    price = float(order['price'])
    leverage = float(order['leverage'])
    margin_type = order['margin_type']

    order_value = price * amount
    order_margin = order_value / leverage

    # case 1. No current position -> create new
    if not current_position:

        liq_price = calc_iso_liq_price(
            price,
            leverage,
            side
        )

        return {
            "user_id": user_id,
            "symbol": symbol,
            "amount": amount,
            "entry_price": price,
            "size": order_value,
            "margin": order_margin,
            "leverage": leverage,
            "side": side,
            "pnl": 0,
            "margin_type": margin_type,
            "status": 1,  # open
            "liq_price": liq_price
        }

    # Existing position details
    current_side = current_position['side']
    current_amount = float(current_position['amount'])
    current_entry_price = float(current_position['entry_price'])
    current_margin = float(current_position['margin'])
    current_size = float(current_position['size'])
    current_pnl = float(current_position.get('pnl') or 0)

    # case 2: Same-side -> merge positions
    if current_side == side:
        total_amount = current_amount + amount
        total_value = (current_entry_price * current_amount) + (price * amount)
        avg_entry_price = total_value / total_amount
        total_size = total_amount * avg_entry_price
        total_margin = current_margin + order_margin
        effective_leverage = total_size / total_margin if total_margin else leverage

        # released_margin = calc_released_margin(current_margin, total_margin)
        liq_price = calc_iso_liq_price(
            avg_entry_price,
            round(effective_leverage, 4),
            side
        )

        return {
            "user_id": user_id,
            "symbol": symbol,
            "amount": total_amount,
            "entry_price": avg_entry_price,
            "size": total_size,
            "pnl": current_pnl,
            "margin": total_margin,
            "leverage": round(effective_leverage, 4),
            "side": side,
            "margin_type": margin_type,
            "status": 1,
            "liq_price": liq_price
        }

    # 🔁 Case 3: Opposite-side → partial close, full close, or flip
    if amount < current_amount:
        # Partial close — reduce position
        new_amount = current_amount - amount
        close_pnl = (price - current_entry_price) * amount if current_side == 'buy' else (
                                                                                                     current_entry_price - price) * amount
        new_pnl = current_pnl + close_pnl
        new_margin = current_margin * (new_amount / current_amount)
        new_size = new_amount * current_entry_price

        effective_leverage = current_size / current_margin if current_margin else leverage

        # released_margin = calc_released_margin(current_margin, new_margin)
        liq_price = calc_iso_liq_price(
            current_entry_price,
            round(effective_leverage, 4),
            current_side
        )

        return {
            "user_id": user_id,
            "symbol": symbol,
            "amount": new_amount,
            "entry_price": current_entry_price,
            "size": new_size,
            "margin": new_margin,
            "leverage": current_size / current_margin if current_margin else leverage,
            "side": current_side,
            "margin_type": margin_type,
            "pnl": new_pnl,
            "close_pnl": close_pnl,
            "status": 1,
            "liq_price": liq_price,
        }

    elif amount == current_amount:
        # Full close — no new position
        close_pnl = (price - current_entry_price) * amount if current_side == 'buy' else (current_entry_price - price) * amount
        new_pnl = current_pnl + close_pnl

        return {
            "user_id": user_id,
            "symbol": symbol,
            "amount": 0,
            "entry_price": None,
            "size": 0,
            "margin": 0,
            "leverage": 0,
            "side": current_side,
            "margin_type": margin_type,
            "pnl": new_pnl,
            "close_pnl": close_pnl,
            "status": 3,  # fully closed
            "liq_price": None
        }

    else:
        # Flip — close current, open new opposite
        flip_amount = amount - current_amount
        close_pnl = (price - current_entry_price) * current_amount if current_side == 'buy' else (
                                                                                                             current_entry_price - price) * current_amount
        new_pnl = close_pnl + current_pnl
        new_value = price * flip_amount
        new_margin = new_value / leverage

        liq_price = calc_iso_liq_price(
            price, leverage, side)

        return {
            "user_id": user_id,
            "symbol": symbol,
            "amount": flip_amount,
            "entry_price": price,
            "size": new_value,
            "margin": new_margin,
            "leverage": leverage,
            "side": side,  # now flipped
            "margin_type": margin_type,
            "pnl": new_pnl,
            "close_pnl": close_pnl,
            "status": 1,
            "opposite": True,
            "liq_price": liq_price
        }


# @router.get('/openLimitOrder', summary='settle limit orders', tags=['EXECUTE API'])
# async def api_executeOpenLimit():
#     mysql = MySQLAdapter()
#     try:
#         price_rows = mysql.get_price()
#         price_dict = {row['symbol']: row for row in price_rows}
#         print(price_dict)
#
#         open_orders = mysql.get_limit_orders_by_status(0)
#         if not open_orders:
#             print("No open orders found")
#             return {"message": "no open orders found"}
#         else:
#             print(f"open orders: {open_orders}")
#
#         for order in open_orders:
#             print("[Processing order]: ", order)
#             order_id = order["id"]
#             user_id = order["user_id"]
#             symbol = order["symbol"]
#             order_price = order["price"]
#             current_price = price_dict.get(symbol, {}).get("price")
#             side = order["side"]
#
#             if current_price is None:
#                 continue
#
#             if (side == 'buy' and order_price >= current_price) or (side == 'sell' and order_price <= current_price):
#
#                 # 0. retrieve the user's wallet balance before executing further logic
#                 wallet_balance = mysql.get_user_balance(user_id)
#
#                 # 1. Retrieve the current position status
#                 current_position = mysql.get_current_position(user_id, symbol)
#                 print(f"[current_position]: {current_position}")
#                 # 2. Calculate the new position with a method
#                 new_position = calculate_position(current_position, order)
#                 # if new_position.get("opposite"):
#                 #     projected_balance = wallet_balance + new_position["close_pnl"]
#                 #     if new_position["margin"] > projected_balance:
#                 #         print("Insufficient margin after flip")
#                 #         continue
#
#                 print(f"new position: {new_position}")
#                 if not current_position:
#                     mysql.insert_position(new_position)
#                 else:
#                     mysql.insert_position(new_position, current_position['id'])
#                 close_pnl = new_position.get('close_pnl')
#                 print('close_pnl: ', close_pnl)
#                 if close_pnl:
#                     new_balance = wallet_balance + close_pnl
#                     if new_balance < 0:
#                         print(f"Wallet balance of the user {user_id} going negative")
#                         continue
#                     update_response = mysql.update_pnl(user_id, new_balance)
#                     if "error" in update_response:
#                         print("update_pnl failed:", update_response["error"])
#                         continue
#
#                 # 3. Update order status to 1 (settled)
#                 mysql.update_status(order_id, 1)
#
#         return {
#             "message": "recalculated positions with triggered limit orders",
#         }
#
#     except Exception as e:
#         print(f"Error settling limit orders: {str(e)}")
#         traceback.print_exc()
#         return {"error": str(e)}


trader = TradingService()
mysql = MySQLAdapter()

@router.get('/settleLimitOrders', summary='settle limit orders', tags=['EXECUTE API'])
async def api_settleLimitOrders():
    count = trader.settle_limit_orders()
    return {"settled orders": count}


@router.get('/settleTpslOrders', summary='settle tpsl orders', tags=['EXECUTE API'])
async def api_settleTpslORders():
    count = trader.settle_tpsl_orders()
    return { "settled orders": count }


@router.post('/close', summary='close existing position', tags=["EXECUTE API"])
def api_closePosition(user_id: int, symbol: str):
    try:
        query_result = mysql.close_position(user_id, symbol)
        return query_result
    except Exception as e:
        print(str(e))
        return {"error": f"Failed to close the existing position {str(e)}"}

@router.post('/liquidate', summary='liquidate position where condition met', tags=['EXECUTE API'])
def api_liquidatePositions():
    try:
        count = mysql.liquidate_positions()
        return {"number of liquidated positions": count }
    except Exception as e:
        print(str(e))
        traceback.print_exc()
        return {"error": f"Error while liquidating positions: {str(e)}"}

@router.post('/calculate_upnl', summary='calculate unrealized pnl of active positions', tags=['EXECUTE API'])
def api_calculateUpnl():
    try:
        count = mysql.calculate_unrealized_pnl()
        return {"number of unrealized pnl derived": count }
    except Exception as e:
        print(str(e))
        traceback.print_exc()
        return { "error": f"Error while calculating unrealized pnl"}
