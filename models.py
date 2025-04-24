from enum import Enum


class OrderModel(str, Enum):
    n1 = 'all'
    n2 = 'limit'
    n3 = 'stop-limit'
   