import os
import sys
import traceback

import pymysql.cursors
from pymysql.cursors import DictCursor
import json
from pymysql.connections import Connection
from starlette.config import Config
from datetime import datetime,timedelta
from pytz import timezone
import pytz
# from boto3 import client
from base64 import b64decode
from utils.make_error import MakeErrorType
import pandas as pd
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
warnings.filterwarnings('ignore')
import redis
import math

from utils.fixed_price_cache import prices as price_cache

config = Config(".env")

# AWS_KMS_KEY_ID = config.get('AWS_KMS_KEY_ID')
# AWS_KMS_KEY_ID = config.get('AWS_KMS_KEY_ID')
# print(config.get('USER_NAME'))
# print(config.get('HOST'))
# print(config.get('PASSWORD'))
# print(config.get('DBNAME'))

class MySQLAdapter:
    def __init__(self) -> None:

        # self.KMS_CLIENT= client("kms", region_name='ap-northeast-2')
        self.exchange_id = 1
        self.now = datetime.now(timezone('Asia/Seoul'))
        self.return_dict_data=dict(results=[], reCode=1, message='Server Error')
        self.status_code=200
        self.status=0
        self.check=0
        self.price1=0

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
            print(e)
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
            print(e)
        else:
            return connection


    def inser_oder_history(self, user_no, symbol, order_type, margin_type, side, price, margin, amount, leverage, status,order_price,tp,sl,po_id=0):
        conn = self._get_connection()

        # 현재 시간을 datetime 객체로 가져오기
        aaa=datetime.strftime(self.now,"%Y-%m-%d %H:%M:%S")
        aaa1=datetime.strptime(aaa,"%Y-%m-%d %H:%M:%S")  # 문자열 형식으로 변환

        try:
            if conn:
                with conn.cursor() as cursor:
                    # 쿼리에서 타이핑 오류 수정: usder_id -> user_id
                    sql = """
                    INSERT INTO order_history
                    (user_id, symbol, type, margin_type, side, price, magin,amount, leverage, status, insert_time, update_time,order_price,po_id,tp,sl) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s)
                    """
                    # cursor.execute를 통해 인자 전달
                    cursor.execute(sql, (user_no, symbol, order_type, margin_type, side, price, margin, amount, leverage, status, aaa1, aaa1,order_price,po_id,tp,sl))
                conn.commit()  # 트랜잭션 커밋

        except Exception as e:
            print("Database error:", e)

        finally:
            if conn:
                conn.close()  # 항상 연결 종료


    def insert_trade_log(self, user_no, symbol, order_type, margin_type, side, price, margin, amount, leverage, tp, sl, status, message):
        conn = self._get_connection()

        # 현재 시간 datetime 객체로 직접 사용
        now = datetime.now()

        try:
            if conn:
                with conn.cursor() as cursor:
                    sql = """
                    INSERT INTO order_log
                    (user_id, symbol, type, margin_type, side, price, magin, amount, leverage, insert_time, update_time, tp, sl, status, message) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql, (
                        user_no, symbol, order_type, margin_type, side,
                        price, margin, amount, leverage,
                        now, now, tp, sl, status, message
                    ))
                    conn.commit()

            else:
                print("Database connection failed.")

        except Exception as e:
            print("Database error:", str(e))

        finally:
            if conn:
                conn.close()


    def inser_position_history(self, user_no, symbol, size,amount, entry_price, liq_price, margin_ratio, margin, pnl,margin_type,side ,leverage, status,tp,sl,close_price):

        conn = self._get_connection()

        # 현재 시간을 datetime 객체로 가져오기
        aaa=datetime.strftime(self.now,"%Y-%m-%d %H:%M:%S")
        aaa1=datetime.strptime(aaa,"%Y-%m-%d %H:%M:%S")  # 문자열 형식으로 변환

        try:
            if conn:
                with conn.cursor() as cursor:
                    # 쿼리에서 타이핑 오류 수정: usder_id -> user_id
                    sql = """
                    INSERT INTO position_history
                    (user_id, symbol, size,amount, entry_price, liq_price, margin_ratio, margin, pnl,margin_type,side ,leverage, status,tp,sl,datetime,close_price) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s, %s)
                    """
                    # cursor.execute를 통해 인자 전달
                    cursor.execute(sql, (user_no, symbol, size, amount, entry_price, liq_price, margin_ratio, margin, pnl,margin_type,side ,leverage, status,tp,sl,aaa1,close_price))

                    conn.commit()  # 트랜잭션 커밋

        except Exception as e:
            print("Database error:", e)

        finally:
            if conn:
                conn.close()  # 항상 연결 종료

    def inser_user_balance(self, user_no, bal):
        conn = self._get_connection()

        # 현재 시간을 datetime 객체로 가져오기
        aaa=datetime.strftime(self.now,"%Y-%m-%d %H:%M:%S")
        aaa1=datetime.strptime(aaa,"%Y-%m-%d %H:%M:%S")  # 문자열 형식으로 변환

        try:
            if conn:
                with conn.cursor() as cursor:
                    # 쿼리에서 타이핑 오류 수정: usder_id -> user_id
                    sql = """
                    INSERT INTO user_balance_history
                    (user_id,balance,datetime ) 
                    VALUES (%s, %s, %s )
                    """
                    # cursor.execute를 통해 인자 전달
                    cursor.execute(sql, (user_no, bal, aaa1))

                    conn.commit()  # 트랜잭션 커밋

        except Exception as e:
            print("Database error:", e)

        finally:
            if conn:
                conn.close()  # 항상 연결 종료


    def get_signal(self):

        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]

        try:
            if conn:
                with conn.cursor() as cursor:

                    sql = f"SELECT * FROM user"

                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    conn.close()
                    print(result)
            #         if len(result)>0:
            #             new_result = result[(result['signal'] == 'S') | (result['signal'] == 'L')]

            #             if len(new_result)>0:
            #                 print(new_result)
            #                 for i in new_result.iterrows():
            #                     new_dict={}
            #                     data=i[1]
            #                     # print(data)
            #                     new_dict['symbol']=data['symbol']
            #                     new_dict['signal']=data['signal']
            #                     new_dict['datetime']=time.mktime((datetime.strptime((data['datetime']),"%Y%m%d%H")-timedelta(hours=9)).timetuple())

            #                     new_list.append(new_dict)


            # self.return_dict_data=dict(results=new_list)
            # self.return_dict_data['reCode']=0
            # self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            # self.status_code=200   



        except Exception as e:
            print(e)
            pass


        return True

    def update_positon(self,id):
        # return_num = 0
        conn = self._get_connection()
        # aaa=datetime.strftime(self.now,"%Y-%m-%d %H:%M:%S")

        # new_aaa=datetime.strftime((self.now-timedelta(hours=4)),"%Y%m%d%H")

        try:
            if conn:
                with conn.cursor() as cursor:

                    sql = """UPDATE position_history SET status = %s WHERE id = %s"""

                    # 파라미터를 튜플로 전달 (symbol을 마지막으로 전달)
                    values = (2,id)

                    # 쿼리 실행
                    cursor.execute(sql, values)

                    # 커밋 후 커넥션 종료
                    conn.commit()

                # 커넥션 종료는 with 블록 밖에서
                conn.close()
        except Exception as e:
            print(e)
            pass

    
    def get_position_list(self,user_no,symbol):
        
        
        
        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]
       
        try:
            if conn:
                with conn.cursor() as cursor:
                    
                    if symbol=='':
                    
                        sql = f"SELECT * FROM  position_history ph WHERE user_id ={user_no} and status =1 order by datetime desc;"
                    else:
                        
                        sql = f"SELECT * FROM  position_history ph WHERE user_id ={user_no} and status =1 and symbol='{symbol}' order by datetime desc;"
                        
                
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    
                    new_dict={}
                if len(result)>0:
                    for i in result.iterrows():
                        df_data=i[1]
                        new_dict={}
                        new_dict['position_id']=df_data['id']
                        new_dict['user_no']=df_data['user_id']
                        new_dict['symbol']=df_data['symbol']
                        new_dict['size']=str(df_data['size'])
                        # new_dict['deposit']=str(df_data['deposit'])
                        new_dict['amount']=str(df_data['amount'])
                        new_dict['entry_price']=str(df_data['entry_price'])
                        new_dict['liq_price']=str(df_data['liq_price'])
                        new_dict['margin']=str(df_data['margin'])
                        new_dict['margin_type']=str(df_data['margin_type'])
                        new_dict['side']=str(df_data['side'])
                        new_dict['leverage']=str(df_data['leverage'])
                        new_dict['tp']=str(df_data['tp'])
                        new_dict['sl']=str(df_data['sl'])
                        new_dict['datetime']=str(df_data['datetime'])
                        new_list.append(new_dict)
                    
                   
                    
                 
                    
           
        
        except Exception as e:
            print(e)
            pass 
        
     
        return new_list
    

    def get_position_chck(self,user_no,symbol):


        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]

        try:
            if conn:
                with conn.cursor() as cursor:

                    sql = f"SELECT * FROM position_history where status=1 and user_id={user_no} and symbol='{symbol}';"

                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    conn.close()
                    # print(result)

                    if len(result)>0:


                        return True

                    else:

                        return False




        except Exception as e:
            print(e)
            pass


    def get_position_return(self,user_no,symbol):


        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]

        try:
            if conn:
                with conn.cursor() as cursor:

                    sql = f"SELECT * FROM position_history where status=1 and user_id={user_no} and symbol='{symbol}' ORDER  BY `datetime` DESC limit 1 ;"

                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    conn.close()
                    # print(result)

                    if len(result)>0:

                        
                        
                        return result,True
                    
                    else:
                        
                        return result,False
            

        except Exception as e:
            print(e)
            pass




    def get_check_user(self,user_no):


        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]

        try:
            if conn:
                with conn.cursor() as cursor:

                    sql = f"SELECT * FROM user where retri_id='{user_no}' and status=0;"

                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    conn.close()
                    # print(result)

                    if len(result)>0:


                        return result

                    else:
                        return []

        except Exception as e:
            print(e)
            pass



    def get_user(self,user_no):


        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]

        try:
            if conn:
                with conn.cursor() as cursor:

                    sql = f"SELECT * FROM user where id={user_no}"


                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)




                    print(result)

                    if len(result)>0:

                        return True

                    else:
                        return False
        except Exception as e:
            print(e)
            pass


    def check_magin_mode(self,user_no,symbol,margin_type):


        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]

        try:
            if conn:
                with conn.cursor() as cursor:

                    sql = f"SELECT * FROM position_history where user_id={user_no} and symbol='{symbol}' and status=1 and margin_type ='{margin_type}';"


                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)




                    print(result)

                    if len(result)>0:

                        return True

                    else:
                        return False
        except Exception as e:
            print(e)
            pass



    def get_diff_balance(self,user_no):


        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]

        try:
            if conn:
                with conn.cursor() as cursor:

                    
                    # sql = f"SELECT * FROM order_history where user_id={user_no} and status =0"
                    sql =f"SELECT * FROM order_history where user_id={user_no} and status =0 AND `type` !='tp' and `type` !='sl';"
                

                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)

                    sql1 = f"SELECT * FROM position_history where user_id={user_no} and status =1"
                    cursor.execute(sql1)
                    result1=cursor.fetchall()
                    result1=pd.DataFrame(result1)


                    balance = self.get_user1(user_no)
                    print(result)
                    print(result1)
                    if len(result)>0:
                        order_bal=sum(result['magin'])
                    else:
                        order_bal=0


                    if len(result1)>0:
                        po_bal=sum(result1['margin'])
                    else:
                        po_bal=0


                    bal=balance-order_bal-po_bal



        except Exception as e:
            print(e)
            pass

        return bal


    def get_ava_balance(self,user_no):


        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]

        try:
            if conn:
                with conn.cursor() as cursor:


                    sql1 = f"SELECT * FROM position_history where user_id={user_no} and status =1 and margin_type='isolated';"
                    cursor.execute(sql1)
                    result1=cursor.fetchall()
                    result1=pd.DataFrame(result1)


                    balance = self.get_user1(user_no)




                    if len(result1)>0:
                        po_bal=sum(result1['margin'])
                    else:
                        po_bal=0


                    bal=balance-po_bal



        except Exception as e:
            print(e)
            pass

        return bal


    def get_side(self,user_no,side,symbol):


        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]

        try:
            if conn:
                with conn.cursor() as cursor:

                    sql = f"SELECT * FROM position_history where status=1 and user_id={user_no} and side = '{side}' and symbol='{symbol}'"


                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)




                    # print(result)

                    if len(result)>0:

                        return True

                    else:
                        return False
        except Exception as e:
            print(e)
            pass



    def get_user1(self,user_no):


        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]

        try:
            if conn:
                with conn.cursor() as cursor:

                    sql = f"SELECT * FROM user where id={user_no} and status=0"

                    sql1 = f"SELECT * FROM user_balance_history where user_id={user_no} order by datetime desc limit 1"

                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)


                    cursor.execute(sql1)
                    result1=cursor.fetchall()
                    result1=pd.DataFrame(result1)
                    conn.close()

                    # print(result)
                    # print(result1)
                    if len(result1)>0:




                        return result1['balance'].iloc[0]

                    else:
                        return result['balance'].iloc[0]



        except Exception as e:
            print(e)
            pass 
        
        
    def update_pnl(self,id,profit,close_price):

        # return_num = 0
        conn = self._get_connection()
        # aaa=datetime.strftime(self.now,"%Y-%m-%d %H:%M:%S")

        # new_aaa=datetime.strftime((self.now-timedelta(hours=4)),"%Y%m%d%H")

        try:
            if conn:
                with conn.cursor() as cursor:

                 
                    sql = """UPDATE position_history SET pnl = %s ,close_price = %s WHERE id = %s"""

                    # 파라미터를 튜플로 전달 (symbol을 마지막으로 전달)
                    values = (profit,close_price,id)

                    # 쿼리 실행
                    cursor.execute(sql, values)

                    # 커밋 후 커넥션 종료
                    conn.commit()

                # 커넥션 종료는 with 블록 밖에서
                conn.close()
        except Exception as e:
            print(e)
            pass

    def calculate_cross_liquidation_price(entry_price, position_usdt, wallet_balance, position_type="long", maintenance_margin_rate=0.005):
        """
        Cross 마진 청산가 계산기 (롱/숏 모두 지원)
        
        :param entry_price: 진입 가격 (ex: 2.1614)
        :param position_usdt: 포지션 규모 (USDT) (ex: 19977.0555)
        :param wallet_balance: 지갑 잔고 (USDT) (ex: 12800)
        :param position_type: 'long' 또는 'short'
        :param maintenance_margin_rate: 유지 마진율 (기본 0.5%)
        :return: 청산 가격
        """
        maintenance_margin = position_usdt * maintenance_margin_rate
        adjusted_balance = wallet_balance - maintenance_margin

        if position_type == "long":
            liquidation_price = entry_price * (1 - adjusted_balance / position_usdt)
        elif position_type == "short":
            liquidation_price = entry_price * (1 + adjusted_balance / position_usdt)
        else:
            raise ValueError("포지션 타입은 'long' 또는 'short' 중 하나여야 합니다.")

        return liquidation_price

        
    # def floor_to_n_decimal(self,x, n):
    #     factor = 10 ** n
    #     print(factor)
    #     return math.floor(x * factor) / factor
    
    def floor_to_n_decimal(self, x, n):
        if not math.isfinite(x):
            print("무한대나 NaN 값이 들어왔습니다.")
            return None  # 또는 적절한 기본값 설정
        factor = 10 ** n
        return math.floor(x * factor) / factor 
        
    
    def get_qty(self,symbol):
        
        
        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]
       
        try:
            if conn:
                with conn.cursor() as cursor:
                    
                    sql = f"SELECT * FROM symbol where symbol='{symbol}';"
                
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    conn.close()
                    # print(result)
                    
                    if len(result)>0:
                        
                        
                        return result['price'].iloc[0],result['qty'].iloc[0]
                    
                    else:
                        return []
            
        except Exception as e:
            print(e)
            pass 
    

    def sell_limit_order(self, user_no: int, symbol: str, margin_type: int, leverage: int,price : float, usdt=0, amount=0,tp=0,sl=0)  :
        print("sell limit order")
        user = self.get_user(user_no)
        check = MakeErrorType()
        rd = self._get_redis()
        # margin_type을 'isolated' 또는 'cross'로 설정

        new_price = rd.get(f'price:{symbol}USDT')
        price_ch,qty_ch=self.get_qty(symbol)

        if new_price:  # price 값이 None이 아닌 경우에만 진행
            new_price1 = float(new_price.decode())  # 바이트 문자열을 디코딩하여 float로 변환'
            # new_price1 = float(new_price)

        margin_type1=margin_type
        usdt1=usdt
        margin_type = 'isolated' if margin_type == 0 else 'cross'
        print('user',user_no, 'symbol',symbol,'margin_type', margin_type,'lever', leverage,'price',price,'usdt', usdt,'amount', amount)

        if    margin_type=='isolated':
            new_margin_type='cross'
        else:
            new_margin_type='isolated'

        check_magin_type=self.check_magin_mode(user_no,symbol,new_margin_type)
        print("check_magin_type",check_magin_type, new_margin_type)
        if check_magin_type ==True:

            self.return_dict_data['reCode']=103
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423

            return False
        try:
            if user:  # user가 True인 경우에만 처리
                balance=self.get_diff_balance(user_no)

                position_chck=self.get_position_list(user_no,symbol)

                if price:  # price 값이 None이 아닌 경우에만 진행

                    if tp !=0:
                        if price < tp :

                            self.return_dict_data['results']=[]
                            self.return_dict_data['reCode']=30006
                            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                            self.status_code=423


                            return False
                    if sl != 0:
                        if price >sl :
                            self.return_dict_data['results']=[]
                            self.return_dict_data['reCode']=30006
                            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                            self.status_code=423

                            return False



                    if usdt > 0:  # usdt를 사용하는 경우

                        print('여기')
                        susu=usdt*0.0002
                        usdt=usdt-susu
                        new_usdt = usdt / leverage  # 새로운 USDT 계산
                        new_amount = usdt / price  # 새로운 금액 계산
                        print("new_amount",new_amount)
                        print("new_usdt",new_usdt)

                    elif amount > 0:  # amount를 사용하는 경우

                        print('여기1')
                        usdt=(amount * price)
                        susu=usdt*0.0002
                        usdt=usdt-susu
                        new_usdt = ((amount * price) / leverage) -susu # 새로운 USDT 계산
                        new_amount = amount  # 금액은 그대로 사용

                        print("new_amount",new_amount)
                        print("new_usdt",new_usdt,usdt)
                        print('usdt',usdt)
                    # 주문 기록 삽입

                    if len(position_chck)>0:

                        position_magin=position_chck[0]['size']
                        if position_chck[0]['side']=='buy':
                            position_side=True

                        else:
                            position_side=False


                    else:
                        position_magin=0
                        position_side=False


                    if position_side ==True:

                        new_balance=float(balance)-new_usdt +float(position_magin)


                    else:

                        new_balance=float(balance)-new_usdt


                    print('new_balance',new_balance)


                    if new_balance >= 0:
                        print('new_balance:',new_balance)


                        if new_price1>price:

                            self.sell_market_order(user_no , symbol, margin_type1, leverage, usdt1, amount,tp,sl)
                        else:

                            self.inser_oder_history(user_no, symbol, 'limit', margin_type, 'sell', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 0,price,tp,sl)

                        self.return_dict_data['results']=[]
                        self.return_dict_data['reCode']=0
                        self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                        self.status_code=200

                    else:
                        print('발란스부족')
                        self.return_dict_data['results']=[]
                        self.return_dict_data['reCode']=104
                        self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                        self.status_code=423
        except Exception as e:
            print(e)
        # return return_data


    def buy_limit_order(self,user_no: int, symbol: str, margin_type: int, leverage: int,price : float, usdt=0, amount=0,tp=0,sl=0 ) :
        user = self.get_user(user_no)
        check = MakeErrorType()

        rd = self._get_redis()
        price_ch,qty_ch=self.get_qty(symbol)
        new_price = rd.get(f'price:{symbol}USDT')

        if new_price:  # price 값이 None이 아닌 경우에만 진행
            # new_price1 = float(new_price.decode())  # 바이트 문자열을 디코딩하여 float로 변환'
            new_price1 = float(new_price)  # 바이트 문자열을 디코딩하여 float로 변환'
        print(f"price of {symbol} is {new_price1}")
        margin_type1=margin_type
        usdt1=usdt
        # margin_type을 'isolated' 또는 'cross'로 설정
        margin_type = 'isolated' if margin_type == 0 else 'cross'
        print('user',user_no, 'symbol',symbol,'margin_type', margin_type,'lever', leverage,'price',price,'usdt', usdt,'amount', amount)

        if    margin_type=='isolated':
            new_margin_type='cross'
        else:
            new_margin_type='isolated'

        price_ch,qty_ch=self.get_qty(symbol)

        check_magin_type=self.check_magin_mode(user_no,symbol,new_margin_type)
        print("check_magin_type",check_magin_type, new_margin_type)
        if check_magin_type ==True:

            self.return_dict_data['reCode']=103
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423

            return False

        try:
            if user:  # user가 True인 경우에만 처리
                balance=self.get_diff_balance(user_no)
                # new_side=self.get_side(user_no,'sell',symbol)
                # print('new-size',new_side)
                # if new_side ==True:
                print("balance: ", balance)


                #     print('실패')
                #     self.return_dict_data['reCode']=30012
                #     self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                #     self.status_code=423

                #     return False


                position_chck=self.get_position_list(user_no,symbol)



                # if  price > new_price1 :

                #     self.return_dict_data['results']=[]
                #     self.return_dict_data['reCode']=30007
                #     self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                #     self.status_code=423



                #     return False

                print('adasdsadas')
                if price:  # price 값이 None이 아닌 경우에만 진행



                    if tp != 0:
                        if price > tp :

                            self.return_dict_data['results']=[]
                            self.return_dict_data['reCode']=30006
                            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                            self.status_code=423



                            return False
                    if sl != 0:
                        if price < sl  :
                            self.return_dict_data['results']=[]
                            self.return_dict_data['reCode']=30006
                            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                            self.status_code=423

                            return False




                    print('sadasdasdas')

                    if usdt > 0:  # usdt를 사용하는 경우

                        print('여기')
                        susu=usdt*0.0002
                        usdt=usdt-susu
                        new_usdt = usdt / leverage  # 새로운 USDT 계산
                        new_amount = usdt / price  # 새로운 금액 계산
                        print("new_amount",new_amount)
                        print("new_usdt",new_usdt)

                    elif amount > 0:  # amount를 사용하는 경우

                        print('여기1')
                        usdt=(amount * price)
                        susu=usdt*0.0002
                        usdt=usdt-susu
                        new_usdt = ((amount * price) / leverage )-susu # 새로운 USDT 계산
                        new_amount = amount  # 금액은 그대로 사용

                        print("new_amount",new_amount)
                        print("new_usdt",new_usdt,usdt)
                        print('usdt',usdt)
                    # 주문 기록 삽입
                    if len(position_chck)>0:

                        position_magin=position_chck[0]['size']
                        if position_chck[0]['side']=='sell':
                            position_side=True

                        else:
                            position_side=False


                    else:
                        position_magin=0
                        position_side=False


                    if position_side ==True:

                        new_balance=float(balance)-new_usdt +float(position_magin)


                    else:

                        new_balance=float(balance)-new_usdt
                    print('new_balance:',new_balance)
                    if new_balance >= 0:


                        if new_price1<price:
                            print('222222222222222222')
                            self.buy_market_order(user_no , symbol, margin_type1, leverage, usdt1, amount,tp,sl)

                        else:
                            print('233333333333333')

                            self.inser_oder_history(user_no, symbol, 'limit', margin_type, 'buy', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 0,price,tp,sl)

                        self.return_dict_data['results']=[]
                        self.return_dict_data['reCode']=0
                        self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                        self.status_code=200

                    else:
                        print('11111발란스부족')
                        self.return_dict_data['results']=[]
                        self.return_dict_data['reCode']=104
                        self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                        self.status_code=423


        except Exception as e:
            traceback.print_exc()
            print(e)

        # return return_data




    def buy_market_order(self, user_no: int, symbol: str, margin_type: int, leverage: int, usdt=0, amount=0,tp=0,sl=0):
        rd = self._get_redis()
        user = self.get_user(user_no)
        check = MakeErrorType()
        usdt1=usdt
        # margin_type을 'isolated' 또는 'cross'로 설정
        margin_type = 'isolated' if margin_type == 0 else 'cross'
        print('user',user_no, 'symbol',symbol,'margin_type', margin_type,'lever', leverage,'usdt', usdt,'amount', amount,'tp',tp,'sl',sl)

        
        
        price_ch,qty_ch=self.get_qty(symbol)
        print('-------------------------------------------------------------',price_ch,qty_ch)

        if    margin_type=='isolated':
            new_margin_type='cross'
        else:
            new_margin_type='isolated'

        check_magin_type=self.check_magin_mode(user_no,symbol,new_margin_type)
        print("check_magin_type",check_magin_type, new_margin_type)

        if check_magin_type ==True:

            self.return_dict_data['reCode']=103
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423

            return False
        try:
            if user:  # user가 True인 경우에만 처리
                # balance = self.get_user1(user_no)

                balance=self.get_diff_balance(user_no)

                
                position_chck=self.get_position_list(user_no,symbol)
                
                
                

                # new_side=self.get_side(user_no,'sell',symbol)
                # print('new-size',new_side, 'balance',balance)
                # if new_side ==True:

                #     print('실패')
                #     self.return_dict_data['reCode']=30012
                #     self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                #     self.status_code=423

                #     return False




                # price 값 가져오기

                price = rd.get(f'price:{symbol}USDT')
                price = float(price.decode())

                

                if price:  # price 값이 None이 아닌 경우에만 진행
                    # price = float(price.decode())  # 바이트 문자열을 디코딩하여 float로 변환
                    self.price1=price
                    print(balance)
                    print(price)

                    
                    
                    if tp !=  0:
                        if price > tp :
                            
                            self.return_dict_data['results']=[]
                            self.return_dict_data['reCode']=30006
                            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                            self.status_code=423
                            
                            
                            
                            return False
                    if sl != 0:
                        if price < sl :
                            self.return_dict_data['results']=[]
                            self.return_dict_data['reCode']=30006
                            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                            self.status_code=423
                            
                            return False
                        
                    
                    
                    
                    

                    if usdt > 0:  # usdt를 사용하는 경우

                        print('여기')
                        susu=usdt*0.0004
                        usdt=usdt-susu
                        new_usdt = usdt / leverage  # 새로운 USDT 계산
                        new_amount = usdt / price  # 새로운 금액 계산
                        print("new_amount",new_amount)
                        print("new_usdt",new_usdt)

                    elif amount > 0:  # amount를 사용하는 경우

                        print('여기1')
                        usdt=(amount * price)
                        susu=usdt*0.0004
                        usdt=usdt-susu
                        new_usdt = (amount * price) / leverage -susu # 새로운 USDT 계산
                        new_amount = amount  # 금액은 그대로 사용
                        
                        print("new_amount",new_amount)
                        print("new_usdt",new_usdt,usdt)
                        print('usdt',usdt)
                    # 주문 기록 삽입

                    
                    
                    
                    # new_balance=float(balance)-new_usdt
                    
                    
                    # if position_chck[0]['side']
                    if len(position_chck)>0:
                        
                        position_magin=position_chck[0]['size']
                        if position_chck[0]['side']=='sell':
                            position_side=True
                        
                        else:
                            position_side=False   
                            
                        
                    else:
                        position_magin=0
                        position_side=False
                        
                        
                    if position_side ==True:
                        
                        new_balance=float(balance)-new_usdt +float(position_magin)
                        
                        
                    else:
                        
                        new_balance=float(balance)-new_usdt 
                    
                    
                    print('new_balance',new_balance)

                    if new_balance >= 0:
                        print('new_balance:',new_balance)

                        if self.get_position_chck(user_no,symbol)==False:

                            print('test')
                            if margin_type=='isolated':

                                
                                liq_price=self.floor_to_n_decimal(price * (1 - (1 / leverage)),price_ch)
                        

                                print('격리 청산가',liq_price,'price',price,'lev',leverage,'마진비율')
                                
                                # self.inser_user_balance(user_no,new_balance)

                                self.inser_position_history(user_no,symbol,self.floor_to_n_decimal(usdt,price_ch),self.floor_to_n_decimal(new_amount,qty_ch),price,liq_price,0,new_usdt,0,margin_type,'buy',leverage,1,0,0,0)
                                position,po=self.get_position_return(user_no,symbol)
                                id=position['id'].iloc[0]
                                self.inser_oder_history(user_no, symbol, 'market', margin_type, 'buy', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 1,price,tp,sl,id)

                            else:
                                print('cross')
                                # position_value = new_amount * price

                                # # 빚은 포지션 가치에서 계좌 잔고를 뺀 값
                                # debt = position_value - balance

                                # liq_price = (debt / (balance + debt)) * price
                                # if liq_price <0:
                                #     liq_price=0
                                # else:
                                #     liq_price=liq_price
                                cross_bal=self.get_ava_balance(user_no)
                                print('cross_bal',cross_bal)

                                maintenance_margin = usdt * 0.005
                                adjusted_balance = cross_bal - maintenance_margin

                                print(usdt, balance)

                                liq_price = self.floor_to_n_decimal(price * (1 - adjusted_balance / usdt),price_ch)
                               
                               

                                print("liq",liq_price)
                                
                                # self.inser_user_balance(user_no,new_balance)

                                    
                                self.inser_position_history(user_no,symbol,self.floor_to_n_decimal(usdt,price_ch),self.floor_to_n_decimal(new_amount,qty_ch),price,liq_price,0,new_usdt,0,margin_type,'buy',leverage,1,0,0,0)
                                position,po=self.get_position_return(user_no,symbol)
                                id=position['id'].iloc[0]
                                self.inser_oder_history(user_no, symbol, 'market', margin_type, 'buy', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 1,price,tp,sl,id)
                                
                            
                            id=position['id'].iloc[0]      
                            if tp !=0:
                                print('tp주문')
                                
                                self.inser_oder_history(user_no, symbol, 'tp', margin_type, 'sell', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 0,price,tp,sl,id)


                            if sl !=0:

                                print('sl주분')

                                self.inser_oder_history(user_no, symbol, 'sl', margin_type, 'sell', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 0,price,tp,sl,id)
                                
                                

                            print('asdasdasdassdadsa')
                            self.return_dict_data['results']=[]
                            self.return_dict_data['reCode']=0
                            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                            self.status_code=200
                            # self.inser_position_history()

                        else:
                            # 물타기

                            print('test1')

                            
                            position,po=self.get_position_return(user_no,symbol)

                            if position['side'].iloc[0]=='buy':
                                print('물타기포지션',position)
                                id=position['id'].iloc[0]
                                new_size=position['size'].iloc[0]+usdt
                                quantity=self.floor_to_n_decimal(position['amount'].iloc[0],qty_ch)+self.floor_to_n_decimal(new_amount,qty_ch)
                                
                              
                                new_p=position['entry_price'].iloc[0]*self.floor_to_n_decimal(position['amount'].iloc[0],qty_ch)
                                new_p1=price*self.floor_to_n_decimal(new_amount,qty_ch)
                                new_p2=new_p+new_p1
                                new_p3=self.floor_to_n_decimal(new_p2/quantity,price_ch)
                                
                                # new_price=self.floor_to_n_decimal(new_size/quantity,price_ch)
                                new_price=new_p3
                                # new_price=self.floor_to_n_decimal(new_p/new_p1,price_ch)
                                print('---------------------------------------------------------------',new_p1,new_p2,quantity,new_size,new_price)
                                new_margin=position['margin'].iloc[0]+new_usdt
                                tp1=position['tp'].iloc[0]
                                sl1=position['sl'].iloc[0]

                                print('물타기 체크', new_size, quantity,new_price,new_margin )

                                if margin_type=='isolated':

                                    
                                    liq_price=self.floor_to_n_decimal(new_price * (1 - (1 / leverage)),price_ch)
                                

                                else:

                                    # position_value = quantity * new_price

                                    # # 빚은 포지션 가치에서 계좌 잔고를 뺀 값
                                    # debt = position_value - balance

                                    # liq_price = (debt / (balance + debt)) * new_price
                                    # if liq_price <0:
                                    #     liq_price=0
                                    # else:
                                    #     liq_price=liq_price
                                    cross_bal=self.get_ava_balance(user_no)
                                    print('cross_bal',cross_bal)
                                    maintenance_margin = new_size * 0.005
                                    adjusted_balance = cross_bal - maintenance_margin

                                    print(usdt, balance)
                                    liq_price =self.floor_to_n_decimal( new_price * (1 - adjusted_balance / new_size),price_ch)
                                self.inser_oder_history(user_no, symbol, 'market', margin_type, 'buy', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 1,price,tp,sl,id)
                                self.inser_position_history(user_no,symbol,self.floor_to_n_decimal(new_size,price_ch),self.floor_to_n_decimal(quantity,qty_ch),new_price,liq_price,0,new_margin,0,margin_type,'buy',leverage,1,tp1,sl1,0)
                                self.update_positon(id)

                                id=position['id'].iloc[0]
                                if tp !=0:
                                    print('tp주문')

                                    
                                    self.inser_oder_history(user_no, symbol, 'tp', margin_type, 'sell', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 0,price,tp,sl,id)


                                if sl !=0:

                                    print('sl주분')

                                    self.inser_oder_history(user_no, symbol, 'sl', margin_type, 'sell', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 0,price,tp,sl,id)
                        

                            else:
                                print('반대포지션')

                                id=position['id'].iloc[0]
                                new_size=position['size'].iloc[0]-usdt
                                quantity=self.floor_to_n_decimal(position['amount'].iloc[0],qty_ch)-self.floor_to_n_decimal(new_amount,qty_ch)
                                new_price=self.floor_to_n_decimal(new_size/quantity,price_ch)
                                new_margin=position['margin'].iloc[0]-new_usdt
                                tp1=position['tp'].iloc[0]
                                sl1=position['sl'].iloc[0]
                                entry_price= position['entry_price'].iloc[0]
                                new_margin1=position['margin'].iloc[0]
                                print( '엔트리 프라이스',entry_price,'뉴마진',new_margin1)

                                # self.inser_oder_history(user_no, symbol, 'market', margin_type, 'buy', price, new_usdt ,new_amount, leverage, 1,price,tp,sl)

                                if amount >0:
                                    if  quantity <0:
                                        print('amount111111111111111111111111111')

                                        if margin_type=='isolated':

                                
                                            liq_price=self.floor_to_n_decimal(entry_price * (1 - (1 / leverage)),price_ch)

                                        else:

                                            cross_bal=self.get_ava_balance(user_no)
                                            print('cross_bal',cross_bal)
                                            maintenance_margin = new_size * 0.005
                                            adjusted_balance = cross_bal- maintenance_margin

                                            print(usdt, balance)

                                            liq_price = self.floor_to_n_decimal(new_price * (1 - adjusted_balance / new_size),price_ch) 
                                        self.cancel_position(user_no,id,new_usdt)
                                        self.inser_position_history(user_no,symbol,abs(self.floor_to_n_decimal(new_size,price_ch)),abs(self.floor_to_n_decimal(quantity,qty_ch)),float(price),float(liq_price),0,abs(new_margin),0,margin_type,'buy',leverage,1,0,0,0)
                                        
                                        position,po=self.get_position_return(user_no,symbol)
                                        id=position['id'].iloc[0]   
                                        if tp !=0:
                                            print('tp주문')
                                            
                                            self.inser_oder_history(user_no, symbol, 'tp', margin_type, 'sell', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 0,price,tp,sl,id)


                                        if sl !=0:

                                            print('sl주분')

                                            self.inser_oder_history(user_no, symbol, 'sl', margin_type, 'sell', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 0,price,tp,sl,id)
                                        

                                        # self.update_positon(id)
                                    elif  quantity==0 :
                                        print('amount22222222222222222222222222222222')
                                        self.cancel_position(user_no,id,new_usdt)
                                    else:
                                        print('amount33333333333333333333333333333333333333333333333')
                                        if margin_type=='isolated':

                                
                                            liq_price=self.floor_to_n_decimal(entry_price * (1 + (1 / leverage)),price_ch)

                                        else:

                                            cross_bal=self.get_ava_balance(user_no)
                                            print('cross_bal',cross_bal)
                                            maintenance_margin = new_size * 0.005
                                            adjusted_balance = cross_bal - maintenance_margin

                                            print(usdt, balance)

                                            liq_price = self.floor_to_n_decimal(new_price * (1 + adjusted_balance / new_size),price_ch)
                                            
                                        # profit=-((price-entry_price)/entry_price)*leverage
                                        # new_profit1=new_margin1*profit 
                                        # print('pnl 체크 ---------------------------------------------',new_profit1)    
                                        
                                        profit=-((price-entry_price)*new_amount)
                                        new_profit1=profit 
                                        print('pnl 체크 ---------------------------------------------',new_profit1)   
                                        
                                        
                                        
                                        balance11=self.get_user1(user_no)
                                        new_balance1=balance11+new_profit1
                                        self.update_bal(new_balance1,user_no) 
                                        
                                        self.inser_oder_history(user_no, symbol, 'market', margin_type, 'buy', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 1,price,tp,sl,id)
                                        self.inser_position_history(user_no,symbol,self.floor_to_n_decimal(new_size,price_ch),self.floor_to_n_decimal(quantity,qty_ch),entry_price,liq_price,0,new_margin,0,margin_type,'sell',leverage,1,tp1,sl1,0)

                                        self.update_positon(id)
                                        self.update_pnl(id,new_profit1,price)
                                if usdt1 >0:
                                    if  new_margin <0:
                                        print('111111111111111111111111111')
                                        if margin_type=='isolated':

                                
                                            liq_price=self.floor_to_n_decimal(entry_price * (1 - (1 / leverage)),price_ch)

                                        else:
                                            print('111111111111111111111111111')
                                            cross_bal=self.get_ava_balance(user_no)
                                            print('cross_bal',cross_bal)
                                            maintenance_margin = new_size * 0.005
                                            adjusted_balance = cross_bal- maintenance_margin

                                            print(usdt, balance)

                                            liq_price =self.floor_to_n_decimal( new_price * (1 - adjusted_balance / new_size) ,price_ch)
                                        self.cancel_position(user_no,id,new_usdt)
                                        self.inser_position_history(user_no,symbol,abs(self.floor_to_n_decimal(new_size,price_ch)),abs(self.floor_to_n_decimal(quantity,qty_ch)),float(price),float(liq_price),0,abs(new_margin),0,margin_type,'buy',leverage,1,0,0,0)
                                        
                                        position,po=self.get_position_return(user_no,symbol)
                                        id=position['id'].iloc[0]   
                                        if tp !=0:
                                            print('tp주문')
                                            
                                            self.inser_oder_history(user_no, symbol, 'tp', margin_type, 'sell', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 0,price,tp,sl,id)


                                        if sl !=0:

                                            print('sl주분')
                                            self.inser_oder_history(user_no, symbol, 'sl', margin_type, 'sell', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 0,price,tp,sl,id)
                                        # self.update_positon(id)
                                    elif  new_margin==0 :
                                        print('22222222222222222222222222222222')
                                        self.cancel_position(user_no,id,new_usdt)
                                    else:
                                        print('33333333333333333333333333333333333333333333333')
                                        if margin_type=='isolated':

                                
                                            liq_price=self.floor_to_n_decimal(entry_price * (1 + (1 / leverage)),price_ch)

                                        else:

                                            cross_bal=self.get_ava_balance(user_no)
                                            print('cross_bal',cross_bal)
                                            maintenance_margin = new_size * 0.005
                                            adjusted_balance = cross_bal - maintenance_margin

                                            print(usdt, balance)
                                            liq_price =self.floor_to_n_decimal( new_price * (1 + adjusted_balance / new_size),price_ch)
                                        # self.close_position(usder_no,id)


                                        # profit=-((price-entry_price)/entry_price)*leverage
                                        # new_profit1=new_margin1*profit 
                                        # print('pnl 체크 ---------------------------------------------',new_profit1)
                                        
                                        profit=-((price-entry_price)*new_amount)
                                        new_profit1=profit 
                                        
                                        balance11=self.get_user1(user_no)
                                        new_balance1=balance11+new_profit1
                                        self.update_bal(new_balance1,user_no) 
                                        
                                        self.inser_oder_history(user_no, symbol, 'market', margin_type, 'buy', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 1,price,tp,sl,id)
                                        self.inser_position_history(user_no,symbol,self.floor_to_n_decimal(new_size,price_ch),self.floor_to_n_decimal(quantity,qty_ch),entry_price,liq_price,0,new_margin,0,margin_type,'sell',leverage,1,tp1,sl1,0)
                                        self.update_positon(id)
                                        self.update_pnl(id,new_profit1,price)
                                        
                            

                            self.return_dict_data['results']=[]
                            self.return_dict_data['reCode']=0
                            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                            self.status_code=200


                    else:
                        print('발란스부족')
                        self.return_dict_data['reCode']=104
                        self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                        self.status_code=423






                else:
                    print(f"가격 정보가 없습니다: price:{symbol}USDT")

        except Exception as e:
            print(f"에러 발생: {e}")

            # return return_data

    # POST Sell Market Order /fapi/v1/order = Weight: 0
    def sell_market_order(self, user_no: int, symbol: str, margin_type: int, leverage: int, usdt=0, amount=0,tp=0,sl=0) :
        check = MakeErrorType()
        rd = self._get_redis()
        user = self.get_user(user_no)
        usdt1=usdt
        print('user',user_no, 'symbol',symbol,'margin_type', margin_type,'lever', leverage,'usdt', usdt,'amount', amount,'tp', tp,'sl', sl)
        # margin_type을 'isolated' 또는 'cross'로 설정
        margin_type = 'isolated' if margin_type == 0 else 'cross'

        if    margin_type=='isolated':
            new_margin_type='cross'
        else:
            new_margin_type='isolated'

            
        price_ch,qty_ch=self.get_qty(symbol)

        check_magin_type=self.check_magin_mode(user_no,symbol,new_margin_type)
        print("check_magin_type",check_magin_type, new_margin_type)
        if check_magin_type ==True:

            self.return_dict_data['reCode']=103
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423

            return False


        print('user',user_no, 'symbol',symbol,'margin_type', margin_type,'lever', leverage,'usdt', usdt,'amount', amount)
        try:
            if user:  # user가 True인 경우에만 처리
                # balance = self.get_user1(user_no)

                balance=self.get_diff_balance(user_no)
                
                position_chck=self.get_position_list(user_no,symbol)
                # new_side=self.get_side(user_no,'buy',symbol)
                # print('new-size',new_side,'balance',balance)
                # if new_side ==True:

                #     print('실패')
                #     self.return_dict_data['reCode']=30012
                #     self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                #     self.status_code=423

                #     return False

                # price 값 가져오기
                price = rd.get(f'price:{symbol}USDT')

                if price:  # price 값이 None이 아닌 경우에만 진행
                    price = float(price.decode())  # 바이트 문자열을 디코딩하여 float로 변환
                    self.price1=price
                    print(balance)
                    print(price)

                    
                    if tp != 0:
                            
                        if price < tp :
                            
                            self.return_dict_data['results']=[]
                            self.return_dict_data['reCode']=30006
                            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                            self.status_code=423
                            
                            
                            
                            return False
                    if sl != 0: 
                        if price > sl :
                            self.return_dict_data['results']=[]
                            self.return_dict_data['reCode']=30006
                            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                            self.status_code=423
                            
                            return False
                        
                    
                    
                    
                    

                    if usdt > 0:  # usdt를 사용하는 경우

                        print('여기')
                        susu=usdt*0.0004
                        usdt=usdt-susu
                        new_usdt = usdt / leverage  # 새로운 USDT 계산
                        new_amount = usdt / price  # 새로운 금액 계산
                        print("new_amount",new_amount)
                        print("new_usdt",new_usdt)

                    elif amount > 0:  # amount를 사용하는 경우

                        print('여기1')
                        usdt=(amount * price)
                        susu=usdt*0.0004
                        usdt=usdt-susu
                        new_usdt = (amount * price) / leverage  # 새로운 USDT 계산
                        new_amount = amount  # 금액은 그대로 사용
                        
                        print("new_amount",new_amount)
                        print("new_usdt",new_usdt,usdt)
                        print('usdt',usdt)
                    # 주문 기록 삽입

                    
                    
                    if len(position_chck)>0:
                        
                        position_magin=position_chck[0]['size']
                        if position_chck[0]['side']=='buy':
                            position_side=True
                        
                        else:
                            position_side=False   
                            
                        
                    else:
                        position_magin=0
                        position_side=False
                        
                        
                    if position_side ==True:
                        
                        new_balance=float(balance)-new_usdt +float(position_magin)
                        
                        
                    else:
                        
                        new_balance=float(balance)-new_usdt 
                    
                    
                    print('new_balance',new_balance)
                    
                    

                    if new_balance >= 0:
                        print('new_balance:',new_balance)

                        if self.get_position_chck(user_no,symbol)==False:

                            print('test')
                            if margin_type=='isolated':

                                
                                liq_price = self.floor_to_n_decimal(price * (1 + (1 / leverage)),price_ch)
                        

                                print('격리 청산가',liq_price,'price',price,'lev',leverage,'마진비율')
                     
                                # self.inser_user_balance(user_no,new_balance)

                                self.inser_position_history(user_no,symbol,self.floor_to_n_decimal(usdt,price_ch),self.floor_to_n_decimal(new_amount,qty_ch),price,liq_price,0,new_usdt,0,margin_type,'sell',leverage,1,0,0,0)
                                position,po=self.get_position_return(user_no,symbol)
                                id=position['id'].iloc[0]
                                self.inser_oder_history(user_no, symbol, 'market', margin_type, 'sell', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 1,price,tp,sl,id)
                            

                            else:

                                # position_value = new_amount * price

                                # # 빚은 포지션 가치에서 계좌 잔고를 뺀 값
                                # debt = position_value - balance

                                # liq_price = (debt / (balance + debt)) * price
                                # if liq_price <0:
                                #     liq_price=0
                                # else:
                                #     liq_price=liq_price
                                cross_bal=self.get_ava_balance(user_no)
                                print('cross_bal',cross_bal)
                                maintenance_margin = usdt * 0.005
                                adjusted_balance = cross_bal - maintenance_margin

                                print(usdt, balance)

                                liq_price = self.floor_to_n_decimal(price * (1 + adjusted_balance / usdt),price_ch)
                                
                   
                                # self.inser_user_balance(user_no,new_balance)
                                self.inser_position_history(user_no,symbol,self.floor_to_n_decimal(usdt,price_ch),self.floor_to_n_decimal(new_amount,qty_ch),price,liq_price,0,new_usdt,0,margin_type,'sell',leverage,1,0,0,0)
                                position,po=self.get_position_return(user_no,symbol)
                                id=position['id'].iloc[0]
                                self.inser_oder_history(user_no, symbol, 'market', margin_type, 'sell', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 1,price,tp,sl,id)
                            position,po=self.get_position_return(user_no,symbol)
                            id=position['id'].iloc[0]   
                            if tp !=0:
                                print('tp주문')
                                
                                self.inser_oder_history(user_no, symbol, 'tp', margin_type, 'buy', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 0,price,tp,sl,id)


                            if sl !=0:

                                print('sl주분')

                                self.inser_oder_history(user_no, symbol, 'sl', margin_type, 'buy', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 0,price,tp,sl,id)   
                                

                            self.return_dict_data['results']=[]
                            self.return_dict_data['reCode']=0
                            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                            self.status_code=200
                            print('asdasdasdassdadsa')

                            # self.inser_position_history()

                        else:
                            # 물타기

                            print('test1')

                            
                            position,po=self.get_position_return(user_no,symbol)

                            print('물타기포지션',position)
                            if position['side'].iloc[0]=='sell':
                                id=position['id'].iloc[0]
                                new_size=position['size'].iloc[0]+usdt
                                quantity=self.floor_to_n_decimal(position['amount'].iloc[0],qty_ch)+self.floor_to_n_decimal(new_amount,qty_ch)
                                # new_price=self.floor_to_n_decimal(new_size/quantity,price_ch)
                                
                                new_p=position['entry_price'].iloc[0]*self.floor_to_n_decimal(position['amount'].iloc[0],qty_ch)
                                new_p1=price*self.floor_to_n_decimal(new_amount,qty_ch)
                                new_p2=new_p+new_p1
                                new_p3=self.floor_to_n_decimal(new_p2/quantity,price_ch)
                                
                                new_price=new_p3
                                new_margin=position['margin'].iloc[0]+new_usdt
                                tp1=position['tp'].iloc[0]
                                sl1=position['sl'].iloc[0]
                                print('물타기 체크', new_size, quantity,new_price,new_margin )

                                if margin_type=='isolated':

                               
                                
                                    liq_price = self.floor_to_n_decimal(price * (1 + (1 / leverage)),price_ch)
                            

                                else:

                                    # position_value = quantity * new_price

                                    # # 빚은 포지션 가치에서 계좌 잔고를 뺀 값
                                    # debt = position_value - balance

                                    # liq_price = (debt / (balance + debt)) * new_price
                                    # if liq_price <0:
                                    #     liq_price=0
                                    # else:
                                    #     liq_price=liq_price
                                    cross_bal=self.get_ava_balance(user_no)
                                    print('cross_bal',cross_bal)
                                    maintenance_margin = new_size * 0.005
                                    adjusted_balance = cross_bal- maintenance_margin

                                    print(usdt, balance)

                                    liq_price = self.floor_to_n_decimal(new_price * (1 + adjusted_balance / new_size),price_ch)
                                    
                                self.inser_oder_history(user_no, symbol, 'market', margin_type, 'sell', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 1,price,tp,sl,id)
                                self.inser_position_history(user_no,symbol,self.floor_to_n_decimal(new_size,price_ch),self.floor_to_n_decimal(quantity,qty_ch),new_price,liq_price,0,new_margin,0,margin_type,'sell',leverage,1,tp1,sl1,0)  
                                self.update_positon(id)
                                
                                position,po=self.get_position_return(user_no,symbol)
                                id=position['id'].iloc[0]   
                                if tp !=0:
                                    print('tp주문')
                                    
                                    self.inser_oder_history(user_no, symbol, 'tp', margin_type, 'buy', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 0,price,tp,sl,id)


                                if sl !=0:

                                    print('sl주분')

                                    self.inser_oder_history(user_no, symbol, 'sl', margin_type, 'buy', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 0,price,tp,sl,id)  
                                    
                                    
                                

                            else:
                                print('반대포지션1111')
                                print('``````````````````````````````````````````````````````````')
                                
                                id=position['id'].iloc[0]
                                new_size=position['size'].iloc[0]-usdt
                                quantity=self.floor_to_n_decimal(position['amount'].iloc[0],qty_ch)-self.floor_to_n_decimal(new_amount,qty_ch)
                                print('**********************',new_size,quantity)
                                new_price=self.floor_to_n_decimal(new_size/quantity,price_ch)
                                print('**********************')
                                new_margin=position['margin'].iloc[0]-new_usdt
                                tp1=position['tp'].iloc[0]
                                sl1=position['sl'].iloc[0]
                                
                                entry_price= position['entry_price'].iloc[0]
                                new_margin1=position['margin'].iloc[0]
                                
                                print('반대포지션', 'new_size',new_size,'quantity',quantity,'new_margig',new_margin)

                                # self.inser_oder_history(user_no, symbol, 'market', margin_type, 'sell', price, new_usdt ,new_amount, leverage, 1,price,tp,sl)
                                # self.inser_user_balance(user_no,new_balance)

                                if amount >0:

                                    if  quantity <0:
                                        print('amount111111111111111111111111111111111')

                                        if margin_type=='isolated':

                               
                                
                                            liq_price=self.floor_to_n_decimal(entry_price * (1 + (1 / leverage)),price_ch)
                                        

                                        else:

                                            cross_bal=self.get_ava_balance(user_no)
                                            print('cross_bal',cross_bal)
                                            maintenance_margin = new_size * 0.005
                                            adjusted_balance = cross_bal- maintenance_margin

                                            print(usdt, balance,new_price)

                                            liq_price = self.floor_to_n_decimal(new_price * (1 + adjusted_balance / new_size) ,price_ch)
                                        self.cancel_position(user_no,id,new_usdt)
                                        self.inser_position_history(user_no,symbol,abs(self.floor_to_n_decimal(new_size,price_ch)),abs(self.floor_to_n_decimal(quantity,qty_ch)),float(price),float(liq_price),0,abs(new_margin),0,margin_type,'sell',leverage,1,0,0,0)
                                        
                                        position,po=self.get_position_return(user_no,symbol)
                                        id=position['id'].iloc[0]   
                                        if tp !=0:
                                            print('tp주문')
                                            
                                            self.inser_oder_history(user_no, symbol, 'tp', margin_type, 'buy', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 0,price,tp,sl,id)


                                        if sl !=0:

                                            print('sl주분')
                                            self.inser_oder_history(user_no, symbol, 'sl', margin_type, 'buy', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 0,price,tp,sl,id)
                                        # self.update_positon(id)

                                    elif  quantity ==0.0 :
                                        

                                        print('amount22222222222222222222222222222222')

                                        self.cancel_position(user_no,id,new_usdt)

                                    else:
                                        print('amount3333333333333333333333333333333')
                                        if margin_type=='isolated':

                               
                                
                                            liq_price=self.floor_to_n_decimal(entry_price * (1 - (1 / leverage)),price_ch)
                                        

                                        else:

                                            cross_bal=self.get_ava_balance(user_no)
                                            print('cross_bal',cross_bal)
                                            maintenance_margin = new_size * 0.005
                                            adjusted_balance = cross_bal - maintenance_margin

                                            print("asdadsadasdsaasdsas",usdt, balance)

                                            liq_price = self.floor_to_n_decimal(new_price * (1 - adjusted_balance / new_size),price_ch)
                                            
                                        # profit=((price-entry_price)/entry_price)*leverage
                                        # new_profit1=new_margin1*profit 
                                        # print('pnl 체크 ---------------------------------------------',new_profit1)    
                                        
                                        profit=((price-entry_price)*new_amount)
                                        new_profit1=profit
                                        balance11=self.get_user1(user_no)
                                        new_balance1=balance11+new_profit1
                                        self.update_bal(new_balance1,user_no)
                                        # self.close_position(usder_no,id)
                                        self.inser_oder_history(user_no, symbol, 'market', margin_type, 'sell', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 1,price,tp,sl,id)
                                        self.inser_position_history(user_no,symbol,self.floor_to_n_decimal(new_size,price_ch),self.floor_to_n_decimal(quantity,qty_ch),entry_price,liq_price,0,new_margin,0,margin_type,'buy',leverage,1,tp1,sl1,0)
                                        self.update_positon(id)
                                        self.update_pnl(id,new_profit1,price)
                                if usdt1 >0:

                                    if  new_margin <0:
                                        print('111111111111111111111111111111111')
                                        if margin_type=='isolated':

                               
                                
                                            liq_price=self.floor_to_n_decimal(entry_price * (1 + (1 / leverage)),price_ch)

                                        else:


                                            cross_bal=self.get_ava_balance(user_no)
                                            print('cross_bal',cross_bal)
                                            maintenance_margin = new_size * 0.005
                                            adjusted_balance = cross_bal- maintenance_margin

                                            print(usdt, balance,new_price)

                                            liq_price = self.floor_to_n_decimal(new_price * (1 + adjusted_balance / new_size) ,price_ch)
                                        self.cancel_position(user_no,id,new_usdt)
                                        self.inser_position_history(user_no,symbol,abs(self.floor_to_n_decimal(new_size,price_ch)),abs(self.floor_to_n_decimal(quantity,qty_ch)),float(price),float(liq_price),0,abs(new_margin),0,margin_type,'sell',leverage,1,0,0,0)
                                        
                                        position,po=self.get_position_return(user_no,symbol)
                                        id=position['id'].iloc[0]   
                                        if tp !=0:
                                            print('tp주문')
                                            
                                            self.inser_oder_history(user_no, symbol, 'tp', margin_type, 'buy', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 0,price,tp,sl,id)


                                        if sl !=0:

                                            print('sl주분')
                                            self.inser_oder_history(user_no, symbol, 'sl', margin_type, 'buy', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 0,price,tp,sl,id)
                                                    # self.update_positon(id)

                                    elif  new_margin ==0.0 :
                                        

                                        print('22222222222222222222222222222222')
                                        self.cancel_position(user_no,id,new_usdt)
                                    else:
                                        print('3333333333333333333333333333333')
                                        if margin_type=='isolated':

                               
                                
                                            liq_price=self.floor_to_n_decimal(entry_price * (1 - (1 / leverage)),price_ch)

                                        else:


                                            cross_bal=self.get_ava_balance(user_no)
                                            print('cross_bal',cross_bal)
                                            maintenance_margin = new_size * 0.005
                                            adjusted_balance = cross_bal - maintenance_margin

                                            print("asdadsadasdsaasdsas",usdt, balance)

                                            liq_price =self.floor_to_n_decimal( new_price * (1 - adjusted_balance / new_size),price_ch)
                                            
                                        # profit=((price-entry_price)/entry_price)*leverage
                                        # new_profit1=new_margin1*profit 
                                        # print('pnl 체크 ---------------------------------------------',new_profit1)       
                                        profit=((price-entry_price)*new_amount)
                                        new_profit1=profit
                                        balance11=self.get_user1(user_no)
                                        new_balance1=balance11+new_profit1
                                        self.update_bal(new_balance1,user_no)


                                        # self.close_position(usder_no,id)
                                        self.inser_oder_history(user_no, symbol, 'market', margin_type, 'sell', price, new_usdt ,self.floor_to_n_decimal(new_amount,qty_ch), leverage, 1,price,tp,sl,id)
                                        self.inser_position_history(user_no,symbol,self.floor_to_n_decimal(new_size,price_ch),self.floor_to_n_decimal(quantity,qty_ch),entry_price,liq_price,0,new_margin,0,margin_type,'buy',leverage,1,tp1,sl1,0)
                                        self.update_positon(id)

                                        self.update_pnl(id,new_profit1,price)
                            
                                
                                

                            # self.update_positon(id)
                            self.return_dict_data['results']=[]
                            self.return_dict_data['reCode']=0
                            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                            self.status_code=200


                    else:
                        print('발란스부족')
                        self.return_dict_data['results']=[]
                        self.return_dict_data['reCode']=104
                        self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                        self.status_code=423




                else:
                    print(f"가격 정보가 없습니다: price:{symbol}USDT")

        except Exception as e:
            print(f"에러 발생: {e}")


        # return return_data




    def update_order(self,id):
        # return_num = 0
        conn = self._get_connection()
        # aaa=datetime.strftime(self.now,"%Y-%m-%d %H:%M:%S")

        # new_aaa=datetime.strftime((self.now-timedelta(hours=4)),"%Y%m%d%H")

        try:
            if conn:
                with conn.cursor() as cursor:

                    sql = """UPDATE order_history SET status = %s WHERE id = %s"""

                    # 파라미터를 튜플로 전달 (symbol을 마지막으로 전달)
                    values = (3,id)

                    # 쿼리 실행
                    cursor.execute(sql, values)

                    # 커밋 후 커넥션 종료
                    conn.commit()

                # 커넥션 종료는 with 블록 밖에서
                conn.close()
        except Exception as e:
            print(e)
            pass


    def get_order_return(self, order_id: int):


        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]

        try:
            if conn:
                with conn.cursor() as cursor:

                    sql = f"SELECT * FROM order_history where id={order_id};"

                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    conn.close()
                    # print(result)

                    if len(result)>0:


                        return result

        except Exception as e:
            print(e)
            pass

    def update_tpsl(self, type, id):
        if type not in ('tp', 'sl'):
            print(f"Invalid type: {type}. Must be 'tp' or 'sl'.")
            return

        conn = None
        try:
            conn = self._get_connection()
            if conn:
                with conn.cursor() as cursor:
                    column = 'tp' if type == 'tp' else 'sl'
                    sql = f"UPDATE position_history SET {column} = %s WHERE id = %s"
                    values = (0, id)
                    cursor.execute(sql, values)
                    conn.commit()
        except Exception as e:
            print(f"Error updating {type} for ID {id}: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception as e:
                    print(f"Error closing connection: {e}")

    def cancel_order(self,user_no, order_id: int) :
        return_data = dict()
        check = MakeErrorType()
        try:
            order=self.get_order_return(order_id)
            print('order',order)
            user = self.get_user(user_no)
            order_type=order['type'].iloc[0]
            order_price=order['order_price'].iloc[0]
            symbol=order['symbol'].iloc[0]

            
            position,po=self.get_position_return(user_no,symbol)
            print('POSI',position)

            if user:
                if po==True :
                    position_id=position['id'].iloc[0]
                    if order_type=='tp' and order_price==0:
                        self.update_tpsl('tp',position_id)


                    elif order_type=='sl' and order_price==0:

                        self.update_tpsl('sl',position_id)





                self.update_order(order_id)
                self.return_dict_data['results']=[]
                self.return_dict_data['reCode']=0
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=200
            else:

                print('not user')

        except Exception as e:
            print(e)


    # def get_all_order_return(self, user_no: int,type:str):
        
        
    #     # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
    #     conn = self._get_connection()
    #     check = MakeErrorType()
    #     new_list=[]
       
    #     try:
    #         if conn:
    #             with conn.cursor() as cursor:
    #                 if type =='all':
    #                     sql = f"SELECT * FROM order_history where user_id={user_no} and status =0;"
    #                 elif type =='limit':
    #                     sql = f"SELECT * FROM order_history where user_id={user_no} and status =0 and type=limit;"
    #                 elif type =='stop-limit':
    #                     sql = f"SELECT * FROM order_history where user_id={user_no} and status =0 and (type=tp or type=sl);"
                
    #                 cursor.execute(sql)
    #                 result=cursor.fetchall()
    #                 result=pd.DataFrame(result)
    #                 conn.close()
    #                 print('*******************************************',result)
                    
    #                 if len(result)>0:
                        
                        
    #                     return result
            
    #     except Exception as e:
    #         print(e)
    #         pass 
    def get_all_order_return(self, user_no: int, type: str):
        conn = self._get_connection()
        new_list = []
        
        try:
            if conn:
                with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                    if type == 'all':
                        sql = "SELECT * FROM order_history WHERE user_id = %s AND status = 0"
                        params = (user_no,)
                    elif type == 'limit':
                        sql = "SELECT * FROM order_history WHERE user_id = %s AND status = 0 AND type = 'limit'"
                        params = (user_no,)
                    elif type == 'stop-limit':
                        sql = "SELECT * FROM order_history WHERE user_id = %s AND status = 0 AND (type = 'tp' OR type = 'sl')"
                        params = (user_no,)
                    else:
                        return pd.DataFrame()  # 잘못된 type일 경우 빈 데이터프레임 반환

                    cursor.execute(sql, params)
                    result = cursor.fetchall()
                    df_result = pd.DataFrame(result)
                    print('*******************************************', df_result)

                    return df_result if not df_result.empty else pd.DataFrame()
        
        except Exception as e:
            print(f"Error in get_all_order_return: {e}")
            return pd.DataFrame()
        
        finally:
            if conn:
                conn.close()
        
    
    def all_cancel_order(self,user_no, order_id: str) :
        return_data = dict()
        check = MakeErrorType()
        print('=========================',order_id)
        try:
            order=self.get_all_order_return(user_no,order_id)
            print('order',order)

            for i in order.iterrows():
                data=i[1]
                order_type=data['type']
                order_price=data['order_price']
                symbol=data['symbol']
                order_id1=data['id']
                position,po=self.get_position_return(user_no,symbol)
                print('POSI',position)
                if po==True :
                    position_id=position['id'].iloc[0]
                    if order_type=='tp' and order_price==0:
                        self.update_tpsl('tp',position_id)
                    elif order_type=='sl' and order_price==0:
                        self.update_tpsl('sl',position_id)
                        
                
                self.update_order(order_id1)
                
                
            self.return_dict_data['results']=[]
            self.return_dict_data['reCode']=0
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=200
     
            
        except Exception as e:
            print(e)        
            
            
            
            
    
    
    def update_close_position(self, pnl, id,close_price):
        """
        포지션 히스토리에서 해당 id의 상태를 '종료(3)'로 설정하고, 손익(pnl)을 업데이트합니다.
        """
        try:
            conn = self._get_connection()
            if not conn:
                print("DB 연결 실패")
                return

            with conn.cursor() as cursor:
                sql = """
                    UPDATE position_history
                    SET status = %s, pnl = %s, close_price= %s
                    WHERE id = %s
                """
                values = (3, pnl, close_price,id)
                print('test',values)
                cursor.execute(sql, values)
                conn.commit()

        except Exception as e:
            print(f"DB 업데이트 오류: {e}")

        finally:
            if conn:
                conn.close()


    def update_bal(self,balance,id):
        # return_num = 0
        conn = self._get_connection()
        # aaa=datetime.strftime(self.now,"%Y-%m-%d %H:%M:%S")

        # new_aaa=datetime.strftime((self.now-timedelta(hours=4)),"%Y%m%d%H")

        try:
            if conn:
                with conn.cursor() as cursor:

                    sql = """UPDATE user SET balance  = %s  WHERE id = %s"""

                    # 파라미터를 튜플로 전달 (symbol을 마지막으로 전달)
                    values = (balance,id)

                    # 쿼리 실행
                    cursor.execute(sql, values)

                    # 커밋 후 커넥션 종료
                    conn.commit()

                # 커넥션 종료는 with 블록 밖에서
                conn.close()
        except Exception as e:
            print(e)
            pass

    def update_order_tp_sl(self,symbol,user_id):
        # return_num = 0
        conn = self._get_connection()
        # aaa=datetime.strftime(self.now,"%Y-%m-%d %H:%M:%S")

        # new_aaa=datetime.strftime((self.now-timedelta(hours=4)),"%Y%m%d%H")

        try:
            if conn:
                with conn.cursor() as cursor:

                    sql = """UPDATE order_history SET status  = %s  where symbol =%s AND (`type` ='tp' or `type`='sl') and user_id=%s and status=0 ;"""

                    # 파라미터를 튜플로 전달 (symbol을 마지막으로 전달)
                    values = (4,symbol,user_id)

                    # 쿼리 실행
                    cursor.execute(sql, values)

                    # 커밋 후 커넥션 종료
                    conn.commit()

                # 커넥션 종료는 with 블록 밖에서
                conn.close()
        except Exception as e:
            print(e)
            pass

            
    
            
            
            

    def cancel_position(self, user_no, position_id: int,usdt:float=0,close_price=0) :

        conn = self._get_connection()
        return_data = dict()
        check = MakeErrorType()
        rd = self._get_redis()
        user = self.get_user(user_no)
        if user:
            try:


                if conn:
                    with conn.cursor() as cursor:

                        sql = f"SELECT * FROM position_history where id={position_id}"



                        cursor.execute(sql)
                        result=cursor.fetchall()
                        result=pd.DataFrame(result)


                        conn.close()

                        print(result)
                        symbol=result['symbol'].iloc[0]
                        price = rd.get(f'price:{symbol}USDT')
                        price = float(price.decode())
                        entry_price= result['entry_price'].iloc[0]
                        if usdt ==0:
                            
                            new_usdt=result['margin'].iloc[0]
                        else:
                            new_usdt=usdt
                        new_amount=result['amount'].iloc[0]
                        margin_type=result['margin_type'].iloc[0]
                        leverage=result['leverage'].iloc[0]
                        print(price,entry_price)

                        

                        if result['side'].iloc[0] == 'buy':
                            print('클로즈 바이')
                            profit=((price-entry_price)/entry_price)*result['leverage'].iloc[0]
                            new_profit=result['margin'].iloc[0]*profit
                            print('profit',profit,'new_profit',new_profit)

                            self.inser_oder_history(user_no, symbol, 'market', margin_type, 'sell', price, new_usdt ,new_amount, leverage, 1,price,0,0,position_id)

                        else:
                            print('클로즈 셀')

                            profit=-((price-entry_price)/entry_price)*result['leverage'].iloc[0]
                            new_profit=result['margin'].iloc[0]*profit
                            print('profit',profit,'new_profit',new_profit)
                            self.inser_oder_history(user_no, symbol, 'market', margin_type, 'buy', price, new_usdt ,new_amount, leverage, 1,price,0,0,position_id)
                        self.update_order_tp_sl(symbol,user_no)
                        self.update_close_position(float(new_profit),position_id,price)
                        balance=self.get_user1(user_no)
                        new_balance=balance+new_profit
                        self.update_bal(new_balance,user_no)
                        print(balance,new_balance)

                
                # self.update_order(position_id)

                self.return_dict_data['results']=[]
                self.return_dict_data['reCode']=0
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=200

            except Exception as e:
                print(e)



    def get_position_return1(self,user_no,position_id):


        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]

        try:
            if conn:
                with conn.cursor() as cursor:

                    sql = f"SELECT * FROM position_history where status=1 and user_id={user_no} and id='{position_id}' ;"

                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    conn.close()
                    # print(result)

                    if len(result)>0:


                        return result

                    else:
                        return []
            

        except Exception as e:
            print(e)
            pass

    # def update_tpsl_position(self, user_no,position_id,tp,sl):
    #     """
    #     포지션 히스토리에서 해당 id의 상태를 '종료(3)'로 설정하고, 손익(pnl)을 업데이트합니다.
    #     """
    #     check = MakeErrorType()
    #     user = self.get_user(user_no)
    #     conn = self._get_connection()
    #     if user:
    #         try:
    #             position=self.get_position_return1(user_no,position_id)
    #             print(position)
    #             id=position['id'].iloc[0]  
    #             symbol=position['symbol'].iloc[0] 
    #             margin_type=position['margin_type'].iloc[0] 
    #             new_usdt=position['margin'].iloc[0]
    #             new_amount=position['amount'].iloc[0]
    #             leverage=position['leverage'].iloc[0]

    #             if tp !=0:
    #                 print('tp주문')

    #                 self.inser_oder_history(user_no, symbol, 'tp', margin_type, 'buy', 0, new_usdt ,new_amount, leverage, 0,0,tp,sl,position_id)

    #                 with conn.cursor() as cursor:
    #                     sql = """
    #                         UPDATE position_history
    #                         SET tp = %s
    #                         WHERE id = %s
    #                     """
    #                     values = (tp, position_id)
    #                     print('test',values)
    #                     cursor.execute(sql, values)
    #                     conn.commit()

    #             if sl !=0:

    #                 print('sl주분')
    #                 self.inser_oder_history(user_no, symbol, 'sl', margin_type, 'buy', 0, new_usdt ,new_amount, leverage, 0,0,tp,sl,position_id)
    #                 with conn.cursor() as cursor:
    #                     sql = """
    #                         UPDATE position_history
    #                         SET sl = %s
    #                         WHERE id = %s
    #                     """
    #                     values = (sl, position_id)
    #                     print('test',values)
    #                     cursor.execute(sql, values)
    #                     conn.commit()



    #             self.return_dict_data['results']=[]
    #             self.return_dict_data['reCode']=0
    #             self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])

    #         except Exception as e:
    #             print(f"DB 업데이트 오류: {e}")

    
    
    def get_tp_sl_return(self,user_no,symbol,type):
        
        
        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]
        result=None
        try:
            if conn:
                with conn.cursor() as cursor:
                    
                    if type == 'tp':
                        sql = f"SELECT * FROM order_history where (status=0 or status=3) and user_id={user_no} and symbol ='{symbol}' and type ='tp' and order_price = 0 ;"
                    elif type =='sl':
                        sql = f"SELECT * FROM order_history where (status=0 or status=3) and user_id={user_no} and symbol ='{symbol}' and type ='sl' and order_price = 0 ;"
                        
                    
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    conn.close()
                    # print(result)
                    
                    if len(result)>0:
                        
                        
                        return result,True
                    else:
                        return result,False
            
        except Exception as e:
            print(e)
            pass  
               
    def update_order_tp_sl_position(self,price,id,type):
        # return_num = 0
        conn = self._get_connection()
        # aaa=datetime.strftime(self.now,"%Y-%m-%d %H:%M:%S")
        
        # new_aaa=datetime.strftime((self.now-timedelta(hours=4)),"%Y%m%d%H")
        
        try:
            if conn:
                with conn.cursor() as cursor:
                    if type == 'tp':
                        sql = """UPDATE order_history SET   tp= %s  where id=%s ;"""
                    else:
                        sql = """UPDATE order_history SET   sl= %s  where id=%s ;"""
                        
                    # 파라미터를 튜플로 전달 (symbol을 마지막으로 전달)
                    values = (price,id)

                    # 쿼리 실행
                    cursor.execute(sql, values)

                    # 커밋 후 커넥션 종료
                    conn.commit()
                
                # 커넥션 종료는 with 블록 밖에서
                conn.close()   
        except Exception as e:
            print(e)
            pass
        
    
    
    def update_order_tp_sl_status(self,status,id):
        # return_num = 0
        conn = self._get_connection()
        # aaa=datetime.strftime(self.now,"%Y-%m-%d %H:%M:%S")
        
        # new_aaa=datetime.strftime((self.now-timedelta(hours=4)),"%Y%m%d%H")
        
        try:
            if conn:
                with conn.cursor() as cursor:
                   
                    sql = """UPDATE order_history SET   status= %s  where id=%s ;"""
                    
                        
                    # 파라미터를 튜플로 전달 (symbol을 마지막으로 전달)
                    values = (status,id)

                    # 쿼리 실행
                    cursor.execute(sql, values)

                    # 커밋 후 커넥션 종료
                    conn.commit()
                
                # 커넥션 종료는 with 블록 밖에서
                conn.close()   
        except Exception as e:
            print(e)
            pass
    
    

    def update_tpsl_position(self, user_no, position_id, tp, sl):
        """
        포지션 히스토리에서 해당 id의 상태를 '종료(3)'로 설정하고, TP/SL 값을 업데이트합니다.
        """
        user = self.get_user(user_no)
        conn = self._get_connection()
        check = MakeErrorType()
        if not user:
            return  # 사용자 없으면 종료

        try:
            position = self.get_position_return1(user_no, position_id)
            print(position)

            pos_data = position.iloc[0]
            symbol = pos_data['symbol']
            margin_type = pos_data['margin_type']
            margin = pos_data['margin']
            amount = pos_data['amount']
            leverage = pos_data['leverage']
            side=pos_data['side']
            
            update_or_tp,update_ch_tp=self.get_tp_sl_return(user_no,symbol,'tp')
            update_or_sl,update_ch_sl=self.get_tp_sl_return(user_no,symbol,'sl')
            
            print( 'update_or_tp',update_or_tp , update_ch_tp)
            print('update_or_sl',update_or_sl,update_ch_sl )
            def insert_order_and_update(field_name, value, label,side,new_type,new):
                if new==1:
                    if value != 0:
                        print(f'{label} 주문')
                        if side=='buy':
                            new_side='sell'
                        else:
                            new_side='buy'
                        if new_type == 1:
                            self.inser_oder_history(
                                user_no, symbol, label, margin_type, new_side, 0,
                                margin, amount, leverage, 0, 0, tp, sl, position_id
                            )
                        with conn.cursor() as cursor:
                            sql = f"""
                                UPDATE position_history
                                SET {field_name} = %s
                                WHERE id = %s
                            """
                            cursor.execute(sql, (value, position_id))
                            print('업데이트:', (value, position_id))
                            conn.commit()
                            
                else:
                    
                    
                    print(f'{label} 주문')
                    if side=='buy':
                        new_side='sell'
                    else:
                        new_side='buy'
                    if new_type == 1:
                        self.inser_oder_history(
                            user_no, symbol, label, margin_type, new_side, 0,
                            margin, amount, leverage, 0, 0, tp, sl, position_id
                        )
                    with conn.cursor() as cursor:
                        sql = f"""
                            UPDATE position_history
                            SET {field_name} = %s
                            WHERE id = %s
                        """
                        cursor.execute(sql, (value, position_id))
                        print('업데이트:', (value, position_id))
                        conn.commit()

                        
                    
                          
            if update_ch_tp == False:
                insert_order_and_update('tp', tp, 'tp',side,1,1)
            else:
                id=update_or_tp['id'].iloc[0]
                
                if tp !=0:
                    self.update_order_tp_sl_status(0,id)
                    self.update_order_tp_sl_position(tp,id,'tp')
                    insert_order_and_update('tp', tp, 'tp',side,0,2)
                else:
                    self.update_order_tp_sl_status(3,id)
                    insert_order_and_update('tp', tp, 'tp',side,0,2)
            if update_ch_sl== False:
                insert_order_and_update('sl', sl, 'sl',side,1,1)
            else:
                id=update_or_sl['id'].iloc[0]
                
                if sl !=0:
                    self.update_order_tp_sl_status(0,id)
                    self.update_order_tp_sl_position(sl,id,'sl')
                    insert_order_and_update('sl', sl, 'sl',side,0,2)
                else:
                    self.update_order_tp_sl_status(3,id)
                    insert_order_and_update('sl', sl, 'sl',side,0,2)
            

            self.return_dict_data['results']=[]
            self.return_dict_data['reCode']=0
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            # self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=200

        except Exception as e:
            print('오류 발생:', e)

                
          
    
    
    
    
    def all_cancel_position(self, user_no) :
        conn = self._get_connection()
        return_data = dict()
        check = MakeErrorType()
        rd = self._get_redis()
        user = self.get_user(user_no)
        if user:
            try:
                
                
                if conn:
                    with conn.cursor() as cursor:
                        
                        sql = f"SELECT * FROM position_history where user_id={user_no} and status=1"
                        
                    
                    
                        cursor.execute(sql)
                        result=cursor.fetchall()
                        result=pd.DataFrame(result)
                        
                    
                        conn.close()
                        
                        print(result)
                        
                        if len(result) > 0:
                            
                            for i in result.iterrows():
                                data=i[1]
                                id=data['id']
                                symbol=data['symbol']
                                price = rd.get(f'price:{symbol}USDT')
                                price = float(price.decode())
                                entry_price= data['entry_price']
                                new_usdt=data['margin']
                                new_amount=data['amount']
                                margin_type=data['margin_type']
                                leverage=data['leverage']
                                print(price,entry_price)
                                
                            
                                    
                                    
                                
                                if data['side'] == 'buy':
                                    print('클로즈 바이')
                                    profit=((price-entry_price)/entry_price)*data['leverage']
                                    new_profit=data['margin']*profit
                                    print('profit',profit,'new_profit',new_profit)
                                    self.inser_oder_history(user_no, symbol, 'market', margin_type, 'sell', price, new_usdt ,new_amount, leverage, 1,price,0,0,id)
                                    
                                else:
                                    print('클로즈 셀')
                                    
                                    profit=-((price-entry_price)/entry_price)*data['leverage']
                                    new_profit=data['margin']*profit
                                    print('profit',profit,'new_profit',new_profit)
                                    self.inser_oder_history(user_no, symbol, 'market', margin_type, 'buy', price, new_usdt ,new_amount, leverage, 1,price,0,0,id)
                                self.update_order_tp_sl(symbol,user_no)
                                self.update_close_position(float(new_profit),id,price)
                                balance=self.get_user1(user_no)
                                new_balance=balance+new_profit
                                self.update_bal(new_balance,user_no)
                                print(balance,new_balance)
                
                # self.update_order(position_id)
                self.return_dict_data['results']=[]
                self.return_dict_data['reCode']=0
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=200
                
            except Exception as e:
                print(e)














