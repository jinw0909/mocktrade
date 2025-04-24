import os
import sys
import pymysql.cursors
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
            connection = rd = redis.Redis(host='172.31.11.200', port=6379, db=0)
           
            
        except Exception as e:
            print(e)
        else:
            return connection
        
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
                        if df_data['liq_price'] <0 :
                            liq_price=0
                        
                        else:
                            liq_price=df_data['liq_price']
                        
                        new_dict={}
                        new_dict['position_id']=df_data['id']
                        new_dict['user_no']=df_data['user_id']
                        new_dict['symbol']=df_data['symbol']
                        new_dict['size']=str(df_data['size'])
                        # new_dict['deposit']=str(df_data['deposit'])
                        new_dict['amount']=str(df_data['amount'])
                        new_dict['entry_price']=str(df_data['entry_price'])
                        new_dict['liq_price']=str(liq_price)
                        new_dict['margin']=str(df_data['margin'])
                        new_dict['margin_type']=str(df_data['margin_type'])
                        new_dict['side']=str(df_data['side'])
                        new_dict['leverage']=str(df_data['leverage'])
                        new_dict['tp']=str(df_data['tp'])
                        new_dict['sl']=str(df_data['sl'])
                        new_dict['datetime']=str(df_data['datetime'])
                        new_list.append(new_dict)
                    
                   
                    
                 
                    
            self.return_dict_data=dict(results=new_list)
            self.return_dict_data['reCode']=0
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=200   
                    
                       
               
        except Exception as e:
            print(e)
            pass 
        
     
        return True
    
    
    
    def get_order_list(self,user_no):
        
        
        
        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]
       
        try:
            if conn:
                with conn.cursor() as cursor:
                    
                    # sql = f"SELECT * FROM  order_history ph WHERE user_id ={user_no} order by datetime desc;"
                    sql = f"SELECT * FROM order_history WHERE user_id = {user_no} ORDER BY insert_time DESC;"
                
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    
                    new_dict={}
                if len(result)>0:
                    for i in result.iterrows():
                        df_data=i[1]
                        if df_data['type']=='tp':
                            
                            price=df_data['tp']
                        elif df_data['type']=='sl':   
                            
                            price=df_data['sl']      
                        else:
                            price=df_data['price'] 
                        new_dict={}
                        new_dict['ordid']=df_data['id']
                        new_dict['user_no']=df_data['user_id']
                        new_dict['symbol']=df_data['symbol']
                        new_dict['type']=str(df_data['type'])
                        # new_dict['deposit']=str(df_data['deposit'])
                        new_dict['margin_type']=str(df_data['margin_type'])
                        new_dict['side']=str(df_data['side'])
                        new_dict['price']=str(price)
                        new_dict['margin']=str(df_data['magin']*df_data['leverage'])
                        new_dict['amount']=str(df_data['amount'])
                        # new_dict['side']=str(df_data['side'])
                        new_dict['leverage']=str(df_data['leverage'])
                        new_dict['status']=str(df_data['status'])
                        new_dict['datetime']=str(df_data['insert_time'])
                        new_list.append(new_dict)
                    
                   
                    
                 
                    
            self.return_dict_data=dict(results=new_list)
            self.return_dict_data['reCode']=0
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=200   
                    
                       
               
        except Exception as e:
            print(e)
            pass 
        
     
        return True
    
    
    
    def get_openorder_list(self,user_no,symbol):
        
        
        
        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]
       
        try:
            if conn:
                with conn.cursor() as cursor:
                    if symbol=='':
                    # sql = f"SELECT * FROM  order_history ph WHERE user_id ={user_no} order by datetime desc;"
                        sql = f"SELECT * FROM order_history WHERE user_id = {user_no} and status =0 ORDER BY insert_time DESC;"
                    else:
                        sql = f"SELECT * FROM order_history WHERE user_id = {user_no} and status =0 and symbol='{symbol}' ORDER BY insert_time DESC;"
                
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    
                    new_dict={}
                if len(result)>0:
                    for i in result.iterrows():
                        df_data=i[1]
                        if df_data['type']=='tp':
                            
                            price=df_data['tp']
                        elif df_data['type']=='sl':   
                            
                            price=df_data['sl']      
                        else:
                            price=df_data['price'] 
                        
                        new_dict={}
                        new_dict['ordid']=df_data['id']
                        new_dict['user_no']=df_data['user_id']
                        new_dict['symbol']=df_data['symbol']
                        new_dict['type']=str(df_data['type'])
                        # new_dict['deposit']=str(df_data['deposit'])
                        new_dict['margin_type']=str(df_data['margin_type'])
                        new_dict['side']=str(df_data['side'])
                        new_dict['price']=str(price)
                        new_dict['margin']=str(df_data['magin']*df_data['leverage'])
                        new_dict['amount']=str(df_data['amount'])
                        # new_dict['side']=str(df_data['side'])
                        new_dict['leverage']=str(df_data['leverage'])
                        new_dict['tp']=str(df_data['tp'])
                        new_dict['sl']=str(df_data['sl'])
                        new_dict['datetime']=str(df_data['insert_time'])
                        new_list.append(new_dict)
                    
                   
                    
                 
                    
            self.return_dict_data=dict(results=new_list)
            self.return_dict_data['reCode']=0
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=200   
                    
                       
               
        except Exception as e:
            print(e)
            pass 
        
     
        return True
    
    
    
    def get_posioder(self,po_id):
        
        
        
        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]
       
        try:
            if conn:
                with conn.cursor() as cursor:
                    
                    sql = f"SELECT * FROM order_history WHERE po_id = {po_id} and status=1  ORDER BY insert_time DESC;"
                    
                
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    
                    # print('-----------------',result)
                if len(result)>0:
                    
                    data=result
                    
                    
                    if len(result)>=2:
                        
                        data=result
                        
                    
                    return data,True
                
                else:
                
                    return data,False
              
                 
         
                       
               
        except Exception as e:
            print(e)
            pass 
        
     
        
    
    
    
    
    def get_position_history(self,user_no):
        
        
        
        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]
       
        try:
            if conn:
                with conn.cursor() as cursor:
                   
                    # sql = f"SELECT * FROM  order_history ph WHERE user_id ={user_no} order by datetime desc;"
                    sql = f"SELECT * FROM position_history WHERE user_id = {user_no} and (status =2 or status=3) and pnl !=0 ORDER BY datetime DESC;"
               
                
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    print(result)
                #     new_dict={}
                if len(result)>0:
                    for i in result.iterrows():
                        new_dict={}
                        df_data=i[1]
                        order,orty=self.get_posioder(df_data['id'])
                        print('======================================',order,'**********************',orty)
                        
                        if  orty ==True:
                     
                            print('order',order)
                            new_dict['user_no']=df_data['user_id']
                            new_dict['symbol']=df_data['symbol']
                            new_dict['side']=str(df_data['side'])
                            new_dict['margin_type']=str(df_data['margin_type'])
                            # new_dict['deposit']=str(df_data['deposit'])
                            new_dict['close_vol']=str(order['amount'].iloc[0])
                            new_dict['entry_price']=str(df_data['entry_price'])
                            new_dict['close_price']=str(order['price'].iloc[0])
                            new_dict['close_pnl']=str(df_data['pnl'])
                            new_dict['close_datetime']=str(df_data['datetime'])
                            # new_dict['amount']=str(df_data['amount'])
                            # # new_dict['side']=str(df_data['side'])
                            # new_dict['leverage']=str(df_data['leverage'])
                            # new_dict['tp']=str(df_data['tp'])
                            # new_dict['sl']=str(df_data['sl'])
                            # new_dict['datetime']=str(df_data['insert_time'])
                            new_list.append(new_dict)
                    
                   
                    
                 
                    
            self.return_dict_data=dict(results=new_list)
            self.return_dict_data['reCode']=0
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=200   
                    
                       
               
        except Exception as e:
            print(e)
            pass 
        
     
        return True
    
    
    
    def get_diff_balance(self,user_no):
        
        
        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]
       
        try:
            if conn:
                with conn.cursor() as cursor:
                    
                    # sql = f"SELECT * FROM order_history where user_id={user_no} and status =0"
                    sql = f"SELECT * FROM order_history where user_id={user_no} and status =0 AND `type` !='tp' and `type` !='sl';"
                
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
                    
                    
                    if bal <0:
                        
                        new_bal=balance-po_bal
                    
                    else:
                        new_bal=balance-order_bal-po_bal
                        
                            
                        
                        
        except Exception as e:
            print(e)
            pass 
        
        return new_bal
    
    
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
    
    
    
    def get_userbalance_list(self,user_no):
        
        
        try:
            # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
            conn = self._get_connection()
            check = MakeErrorType()
            new_list=[]
            ava_bal=self.get_diff_balance(user_no)
            balance=self.get_user1(user_no)
        
            new_dict={}
            new_dict['avbl']=ava_bal
            new_dict['balance']=balance
            
            new_list.append(new_dict)
        
                    
                        
                    
                        
            self.return_dict_data=dict(results=new_list)
            self.return_dict_data['reCode']=0
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=200   
                        
                       
               
        except Exception as e:
            print(e)
            pass 
        
     
        return True
    
    
    
    def start_user(self, user_no):
        conn = self._get_connection()
        check = MakeErrorType()
        # 현재 시간을 datetime 객체로 가져오기
        aaa=datetime.strftime(self.now,"%Y-%m-%d %H:%M:%S")
        aaa1=datetime.strptime(aaa,"%Y-%m-%d %H:%M:%S")  # 문자열 형식으로 변환

        try:
            if conn:
                with conn.cursor() as cursor:
                    # 쿼리에서 타이핑 오류 수정: usder_id -> user_id
                    sql = """
                    INSERT INTO user
                    (retri_id,balance,datetime,status ) 
                    VALUES (%s, %s, %s, %s )
                    """
                    # cursor.execute를 통해 인자 전달
                    cursor.execute(sql, (user_no, 10000, aaa1,0))

                    conn.commit()  # 트랜잭션 커밋
                    
                    
                    self.return_dict_data['results']=[]
                    self.return_dict_data['reCode']=0
                    self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])

        except Exception as e:
            print("Database error:", e)

        finally:
            if conn:
                conn.close()  # 항상 연결 종료
    
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
    
    
    def get_user_info(self,user_no):
        
        
        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]
       
        try:
            if conn:
                with conn.cursor() as cursor:
                    
                    sql = f"SELECT * FROM user where retri_id='{user_no}' ;"
                
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    conn.close()
                    # print(result)
                    new_dict={}
                    if len(result)>0:
                        
                        new_dict['retri_id']=user_no
                        new_dict['status']=True
                        
                        
                    
                    else:
                        new_dict['retri_id']=user_no
                        new_dict['status']=False
                        
                
                new_list.append(new_dict)
                self.return_dict_data['results']=new_list
                self.return_dict_data['reCode']=0
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            
        except Exception as e:
            print(e)
            pass 
        
        return new_dict
    def get_resetuser_chck(self,user_no):
        
        
        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]
       
        try:
            if conn:
                with conn.cursor() as cursor:
                    
                    sql = f"SELECT * FROM user where retri_id={user_no} and status=0 order by datetime desc limit 1"
        
                
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    
                    if len(result)>0:
                        
                        return result
        except Exception as e:
            print(e)
            pass 
    
    
    def get_resetuser_update(self,user_no):
        
        
        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        
        try:
            if conn:
                with conn.cursor() as cursor:
                 
                    sql = """UPDATE user SET status = %s WHERE id = %s"""
            
                    # 파라미터를 튜플로 전달 (symbol을 마지막으로 전달)
                    values = (1,user_no)

                    # 쿼리 실행
                    cursor.execute(sql, values)

                    # 커밋 후 커넥션 종료
                    conn.commit()
                
                # 커넥션 종료는 with 블록 밖에서
                conn.close()   
        except Exception as e:
            print(e)
            pass
    
    def get_seed_update(self,user_no,seed):
        
        
        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        
        try:
            if conn:
                with conn.cursor() as cursor:
                 
                    sql = """UPDATE user SET balance = %s WHERE retri_id = %s and status =0"""
            
                    # 파라미터를 튜플로 전달 (symbol을 마지막으로 전달)
                    values = (seed,user_no)

                    # 쿼리 실행
                    cursor.execute(sql, values)

                    # 커밋 후 커넥션 종료
                    conn.commit()
                
                # 커넥션 종료는 with 블록 밖에서
                conn.close()   
        except Exception as e:
            print(e)
            pass
    def get_resetseed(self,user_no,seed):
        
        
        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        
        try:
            if conn:
                
                
                user=self.get_user_info(user_no)
                print('user')
                if user['status']==True:
                    
                    data=self.get_check_user(user_no)
                    id=user_id=data['id'].iloc[0]  
                    
                    bal=self.get_user1(id)
                    
                    print('asdasdsa',bal)
                    new_bal=float(bal)+float(seed)
                    
                    self.get_seed_update(user_no,new_bal)
                
                  
                    self.return_dict_data['results']=[]
                    self.return_dict_data['reCode']=0
                    self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                    self.status_code=200
                    
                else:
                    self.return_dict_data['results']=[]
                    self.return_dict_data['reCode']=105
                    self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                    self.status_code=200
                    
                    
        except Exception as e:
            print(e)
            pass
    
      
    
    def reset_user(self, user_no):
        conn = self._get_connection()
        check = MakeErrorType()
        # 현재 시간을 datetime 객체로 가져오기
        aaa=datetime.strftime(self.now,"%Y-%m-%d %H:%M:%S")
        aaa1=datetime.strptime(aaa,"%Y-%m-%d %H:%M:%S")  # 문자열 형식으로 변환

        try:
            if conn:
                user=self.get_resetuser_chck(user_no)
                id=user['id'].iloc[0]
                self.get_resetuser_update(id)
                with conn.cursor() as cursor:
                    # 쿼리에서 타이핑 오류 수정: usder_id -> user_id
                    sql = """
                    INSERT INTO user
                    (retri_id,balance,datetime,status ) 
                    VALUES (%s, %s, %s, %s )
                    """
                    # cursor.execute를 통해 인자 전달
                    cursor.execute(sql, (user_no, 10000, aaa1,0))

                    conn.commit()  # 트랜잭션 커밋
                    
                    
                    
                    
                    self.return_dict_data['results']=[]
                    self.return_dict_data['reCode']=0
                    self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])

        except Exception as e:
            print("Database error:", e)

        finally:
            if conn:
                conn.close()  # 항상 연결 종료
                
    
                
                
    # def get_pnl_history(self,user_no):
        
        
        
    #     # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
    #     conn = self._get_connection()
    #     check = MakeErrorType()
    #     new_list=[]
       
    #     try:
    #         if conn:
    #             with conn.cursor() as cursor:
                    
    #                 # sql = f"SELECT * FROM  order_history ph WHERE user_id ={user_no} order by datetime desc;"
    #                 sql = f"SELECT * FROM position_history WHERE user_id = {user_no} and(status=2 or status=3) and pnl>=0 ORDER BY datetime DESC;"
                
    #                 cursor.execute(sql)
    #                 result=cursor.fetchall()
    #                 result=pd.DataFrame(result)
                    
    #                 new_dict={}
    #             if len(result)>0:
    #                 for i in result.iterrows():
    #                     df_data=i[1]
                        
    #                     new_dict={}
    #                     new_dict['position_id']=df_data['id']
    #                     new_dict['symbol']=df_data['symbol']    
    #                     new_dict['margin_type']=str(df_data['margin_type'])
    #                     new_dict['side']=str(df_data['side'])
    #                     new_dict['price']=str(df_data['entry_price'])
    #                     new_dict['margin']=str(df_data['magin']*df_data['leverage'])
    #                     new_dict['amount']=str(df_data['amount'])
    #                     # new_dict['side']=str(df_data['side'])
    #                     new_dict['leverage']=str(df_data['leverage'])
                      
    #                     new_dict['datetime']=str(df_data['insert_time'])
    #                     new_list.append(new_dict)
                    
                   
                    
                 
                    
        #     self.return_dict_data=dict(results=new_list)
        #     self.return_dict_data['reCode']=0
        #     self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
        #     self.status_code=200   
                    
                       
               
        # except Exception as e:
        #     print(e)
        #     pass 
        
     
        # return True