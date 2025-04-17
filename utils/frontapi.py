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
                        new_dict={}
                        new_dict['ordid']=df_data['id']
                        new_dict['user_no']=df_data['user_id']
                        new_dict['symbol']=df_data['symbol']
                        new_dict['type']=str(df_data['type'])
                        # new_dict['deposit']=str(df_data['deposit'])
                        new_dict['margin_type']=str(df_data['margin_type'])
                        new_dict['side']=str(df_data['side'])
                        new_dict['price']=str(df_data['price'])
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
    
    
    
    def get_diff_balance(self,user_no):
        
        
        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]
       
        try:
            if conn:
                with conn.cursor() as cursor:
                    
                    sql = f"SELECT * FROM order_history where user_id={user_no} and status =0"
        
                
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
    
    
    def get_user1(self,user_no):
        
        
        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]
       
        try:
            if conn:
                with conn.cursor() as cursor:
                    
                    sql = f"SELECT * FROM user where id={user_no}"
                    
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
    
    