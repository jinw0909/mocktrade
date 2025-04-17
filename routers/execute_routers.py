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

router = APIRouter()


def calculate_binance_liq_price(entry_price, leverage, side):
    if side == "buy":
        return entry_price * ( 1 - (1 / leverage))
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
        return None # net zero position

    # Determine side and net amount
    side = "buy" if net_amount > 0 else "sell"
    total_amount = abs(net_amount)

    total_size = sum(o["price"] * o["amount"] for o in settled_orders)
    total_margin = sum((o["price"] * o["amount"]) / o["leverage"] for o in settled_orders)

    weighted_entry_price = total_size / sum(o["amount"] for o in settled_orders)
    effective_leverage = total_size / total_margin
    print(f"total size: {total_size}, total_margin: {total_margin}, weighted_entry_price: {weighted_entry_price}, effective leverage: {effective_leverage}")

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


# @router.get('/openLimitOrder', summary='settle limit orders', tags=['EXECUTE API'])
# async def api_executeOpenLimit():
#     mysql = MySQLAdapter()
#     try:
#         price_rows = mysql.get_price()
#         price_dict = { row['symbol']: row for row in price_rows }
#         print(price_dict)
#
#         open_orders = mysql.get_limit_orders_by_status(0)
#         print(f"open orders: {open_orders}")
#
#         #Group orders that get triggered
#         triggered_orders = []
#
#         for order in open_orders:
#             symbol = order["symbol"]
#             order_price = order["order_price"]
#             current_price = price_dict.get(symbol, {}).get("price")
#             side = order["side"]
#
#             print(f"symbol: {symbol}, order_price: {order_price}, current_price: {current_price}, side: {side}")
#
#             if current_price is None:
#                 continue
#
#             if (side == 'buy' and order_price >= current_price) or (side == 'sell' and order_price <= current_price):
#                  # 1. Update status to 1 (settled)
#                 mysql.update_order_status(order["id"], 1, fill_price=current_price)
#                 triggered_orders.append((order["user_id"], symbol))
#
#         unique_updates = list(set(triggered_orders))
#         if not unique_updates:
#             return { "message": "No limit orders were triggered" }
#         else:
#             print(f"unique_updates: {unique_updates}")
#             print(f"triggered orders: {triggered_orders}")
#
#         for user_id, symbol in unique_updates:
#             # 2. fetch all active orders for this user/symbol
#             settled_orders = mysql.get_settled_orders(user_id, symbol)
#             # 3. recalculate position
#             position = aggregate_binance_style_position(settled_orders)
#
#             if position:
#                 # 4. Insert into position_history
#                 mysql.insert_position_history(position, user_id)
#         return {
#             "message": "Settled and recalculated positions",
#             "updated_positions": unique_updates
#         }
#
#     except Exception as e:
#         print(f"Error settling limit orders: {str(e)}")
#         return {"error": str(e)}

def calculate_position(current_position, order):
    """
    Calculates the resulting position after applying an order (market or filled limit).
    If position flips, calls mysql.position_flip and returns a fresh position.
    """

    user_id = order['user_id']
    symbol = order['symbol']
    side = order['side'] # buy or sell
    amount = float(order['amount'])
    price = float(order['price'])
    leverage = float(order['leverage'])
    margin_type = order['margin_type']

    order_value = price * amount
    order_margin = order_value / leverage

    # case 1. No current position -> create new
    if not current_position:
        return {
            "user_id": user_id,
            "symbol": symbol,
            "amount": amount,
            "entry_price": price,
            "size": order_value,
            "margin": order_margin,
            "leverage": leverage,
            "side": side,
            "margin_type": margin_type,
            "status": 1  # open
        }

    # Existing position details
    current_side = current_position['side']
    current_amount = float(current_position['amount'])
    current_entry_price = float(current_position['entry_price'])
    current_margin = float(current_position['margin'])
    current_size = float(current_position['size'])
    current_pnl = float(current_position['pnl'])

    # case 2: Same-side -> merge positions
    if current_side == side:
        total_amount = current_amount + amount
        total_value = (current_entry_price * current_amount) + (price * amount)
        avg_entry_price = total_value / total_amount
        total_size = total_amount * avg_entry_price
        total_margin = current_margin + order_margin
        effective_leverage = total_size / total_margin if total_margin else leverage

        return {
            "user_id": user_id,
            "symbol": symbol,
            "amount": total_amount,
            "entry_price": avg_entry_price,
            "size": total_size,
            "margin": total_margin,
            "leverage": round(effective_leverage, 4),
            "side": side,
            "margin_type": margin_type,
            "status": 1
        }

    # 🔁 Case 3: Opposite-side → partial close, full close, or flip
    if amount < current_amount:
        # Partial close — reduce position
        new_amount = current_amount - amount
        close_pnl = (price - current_entry_price) * amount if current_side == 'buy' else (current_entry_price - price) * amount
        new_pnl = current_pnl + close_pnl
        new_margin = current_margin * (new_amount / current_amount)
        new_size = new_amount * current_entry_price

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
            "status": 1
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
            "close_pnl" : close_pnl,
            "status": 3  # fully closed
        }

    else:
        # Flip — close current, open new opposite
        flip_amount = amount - current_amount
        close_pnl = (price - current_entry_price) * current_amount if current_side == 'buy' else (current_entry_price - price) * current_amount
        new_pnl = close_pnl + current_pnl
        new_value = price * flip_amount
        new_margin = new_value / leverage

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
            "status": 1
        }


@router.get('/openLimitOrder', summary='settle limit orders', tags=['EXECUTE API'])
async def api_executeOpenLimit():
    mysql = MySQLAdapter()
    try:
        price_rows = mysql.get_price()
        price_dict = { row['symbol']: row for row in price_rows }
        print(price_dict)

        open_orders = mysql.get_limit_orders_by_status(0)
        if not open_orders:
            print("No open orders found")
            return {"message" : "no open orders found"}
        else:
            print(f"open orders: {open_orders}")

        for order in open_orders:
            print("Processing order: ", order)
            order_id = order["id"]
            user_id = order["user_id"]
            symbol = order["symbol"]
            order_price = order["order_price"]
            current_price = price_dict.get(symbol, {}).get("price")
            side = order["side"]

            print(f"symbol: {symbol}, order_id: {order_id}, user_id: {user_id}, order_price: {order_price}, current_price: {current_price}, side: {side}")

            if current_price is None:
                continue

            if (side == 'buy' and order_price >= current_price) or (side == 'sell' and order_price <= current_price):
                # 1. Update price to the current price (settled)
                mysql.update_status(order_id, 1)

                # 1. Retrieve the current position status
                current_position = mysql.get_current_position(user_id, symbol)
                print(f"current_position: {current_position}")
                # 2. Calculate the new position with a method
                new_position = calculate_position(current_position, order)
                print(f"new position: {new_position}")
                if not current_position:
                    mysql.insert_position(new_position)
                else:
                    mysql.insert_position(new_position, current_position['id'])
                close_pnl = new_position['close_pnl']
                if not close_pnl:
                    mysql.apply_pnl(user_id, close_pnl)

        return {
            "message": "recalculated positions with triggered limit orders",
        }

    except Exception as e:
        print(f"Error settling limit orders: {str(e)}")
        traceback.print_exc()
        return {"error": str(e)}


@router.post('/close', summary = 'close existing position', tags = ["EXECUTE API"])
def api_closePosition(user_id: int, symbol: str):
    mysql = MySQLAdapter()
    try:
        query_result = mysql.close_position(user_id, symbol)
        return query_result
    except Exception as e:
        print(str(e))
        return { "error" : f"Failed to close the existing position {str(e)}" }