import asyncio
import os
import sys

import httpx
import pymysql.cursors
import logging

from pymysql.cursors import DictCursor
import json
from pymysql.connections import Connection
from starlette.config import Config
from datetime import datetime, timedelta
from pytz import timezone

import pytz
# from boto3 import client
from base64 import b64decode

from utils.make_error import MakeErrorType
import pandas as pd
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
# from utils.make_error import MakeErrorType
from base64 import b64decode
# from models import *
from decimal import Decimal
import math
import numpy as np
import warnings
import pandas as pd
import numpy as np
import time
import traceback

warnings.filterwarnings('ignore')
import redis

config = Config(".env")


logger = logging.getLogger("uvicorn")


def compute_cross_liq_price(
        entry_price: float,
        amount: float,
        buffer: float,
        side: str
) -> float:

    # guard against zero-size
    if amount == 0:
        return 0.0

    maintenance_rate = 0.01
    # 3) Solve for the price at which equity == 0
    if side == 'buy':
        # long -> liquidate when price falls to this level
        # return max((entry_price - buffer / amount) / (1 - maintenance_rate), 0.0)
        return max(
            entry_price * (1 + 0.01) - buffer / amount,
            0.0
        )

    else:
        # short -> liquidate when price rises to this level
        # return max((entry_price + buffer / amount) / ( 1 + maintenance_rate), 0.0)
        return max(
            entry_price * (1 - 0.01) + buffer / amount,
            0.0
        )


def should_liquidate(
        side: str,
        current_price: float,
        liq_price: float) -> bool:
    """
    Check whether a position has crossed its liquidation threshold.
    :param side: 'buy' for long 'sell' for short
    :param current_price: the latest market price
    :param liq_price: the computed liquidation price
    :return: True if the position should be liquidated now
    """

    if current_price is None:
        return False

    if side == 'buy':
        # long -> liquidate when market <= liq_price
        return current_price <= liq_price
    else:
        # short -> liquidate when market >= liq_price
        return current_price >= liq_price


class MySQLAdapter:

    def __init__(self) -> None:

        # self.KMS_CLIENT= client("kms", region_name='ap-northeast-2')
        self.exchange_id = 1
        self.now = datetime.now(timezone('Asia/Seoul'))
        self.return_dict_data = dict(results=[], reCode=1, message='Server Error')
        self.status_code = 200
        self.status = 0
        self.check = 0

    # DB Connection 확인
    def _get_connection(self):
        try:
            # print(config.get('USER1'))
            # print(config.get('HOST'))
            # print(config.get('PASS'))
            # print(config.get('DBNAME'))
            connection = Connection(host=config.get('HOST'),
                                    user=config.get('USER1'),
                                    password=config.get('PASS'),
                                    database=config.get('DBNAME'),
                                    cursorclass=pymysql.cursors.DictCursor)
            connection.ping(False)

        except Exception as e:
            logger.exception("failed to get db connection")
        else:
            return connection

    def _get_redis(self):
        try:
            # print(config.get('USER1'))
            # print(config.get('HOST'))
            # print(config.get('PASS'))
            # print(config.get('DBNAME'))
            # connection = rd = redis.Redis(host='172.31.11.200', port=6379, db=0)
            connection = rd = redis.Redis(host=config.get("REDIS_HOST"), port=6379, db=0)


        except Exception as e:
            logger.exception("failed to get redis connection")
        else:
            return connection




