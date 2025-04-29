import os
import sys
import pymysql.cursors
import json
from pymysql.connections import Connection
from starlette.config import Config
# from boto3 import client
from base64 import b64decode
from utils.make_error import MakeErrorType
import pandas as pd
# from utils.make_error import MakeErrorType
from base64 import b64decode
# from models import *
from decimal import Decimal
import math
import numpy as np
import time
import logging
from logging.handlers import RotatingFileHandler 

config = Config(".env")

# 로그 포맷 정의
log_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# 파일 핸들러 생성
file_handler = RotatingFileHandler("./logs/auth/auth_method.log", maxBytes=5*1024*1024, backupCount=3)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(log_formatter)

# 기본 로거 설정
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)

# 파일 핸들러 생성
file_handler_db = RotatingFileHandler("./logs/auth/session_db.log", maxBytes=5*1024*1024, backupCount=3)
file_handler_db.setLevel(logging.INFO)
file_handler_db.setFormatter(log_formatter)

# DB 로거 설정
logger_db = logging.getLogger(__name__)
logger_db.setLevel(logging.INFO)
logger_db.addHandler(file_handler_db)

class UserSession:
    def __init__(self) -> None:
        self.data=dict(results=[], message='')
        self.status_code = 200
        self.message = 'Ok'


    # DB Connection 확인
    def _get_connection(self):
        try:
            connection = Connection(
                host=config.get('HOST'),
                user=config.get('USER1'),
                password=config.get('PASS'),
                database=config.get('DBNAME'),
                cursorclass=pymysql.cursors.DictCursor
            )
            connection.ping(False)
            
        except Exception as e:
            logger_db.error(f"Error db connect: {e}")

        else:
            return connection
        

    # 처음 페이지 이동시 타는 api / 유저정보 DB 저장    
    def save_user(self, session, user, pakage, end_data):
        conn = self._get_connection()

        user_info = {
            'session_id': session,
            'user_id': user,
            'pakage': pakage,
            'end_data': end_data
        }

        try:
            if conn:
                with conn.cursor() as cursor:
                    check_sql = "SELECT * FROM session WHERE user_id = %s"
                    cursor.execute(check_sql, (user,))
                    existing = cursor.fetchone()
                    values = (session, pakage, end_data, user)

                    if existing:
                        update_sql = """
                            UPDATE session
                            SET session_id = %s, total_package = %s, end_date = %s
                            WHERE user_id = %s
                        """
                        cursor.execute(update_sql, values)

                    else: 
                        insert_sql  = """
                            INSERT INTO session (session_id, total_package, end_date, user_id)
                            VALUES (%s, %s, %s, %s)
                        """
                        cursor.execute(insert_sql, values)

                    conn.commit()

        except Exception as e:
            logger.error(f"Error saving user session: {e}")
            self.status_code = 500
            self.message = '서버 오류입니다.'
            pass

        finally:
            if conn:
                conn.close()

        return user_info

    
    # 리트리 중복 로그인시 타는 api / session 값 빈값으로 초기화
    async def chk_login(self, session, user, pakage, end_data):
        conn = self._get_connection()

        user_info = {
            'session_id': session,
            'user_id': user,
            'pakage': pakage,
            'end_data': end_data
        }

        session = ""

        try:
            if conn:
                with conn.cursor() as cursor:
                    check_sql = "SELECT * FROM session WHERE user_id = %s"
                    cursor.execute(check_sql, (user,))
                    existing = cursor.fetchone()
                    values = (session, pakage, end_data, user)

                    if existing:
                        update_sql = """
                            UPDATE session
                            SET session_id = %s, total_package = %s, end_date = %s
                            WHERE user_id = %s
                        """
                        cursor.execute(update_sql, values)

                        conn.commit()
                    
                    else:
                        self.message = '존재하지 않는 유저입니다.'
                        self.status_code = 404

        except Exception as e:
            logger.error(f"Error catch duplicate login: {e}")
            self.status_code = 500
            self.message = '서버 오류입니다.'
            pass

        finally:
            if conn:
                conn.close()

    # 처음에 유저 정보 뿌려주는 api
    async def get_user_info(self, user):
        conn = self._get_connection()

        try:
            if conn:
                with conn.cursor() as cursor:
                    check_sql = "SELECT * FROM session WHERE user_id = %s"
                    cursor.execute(check_sql, (user,))
                    existing = cursor.fetchone()

                    if existing:
                        if existing['session_id']:
                            self.data['results'] = { 
                                'user_id': existing['user_id'],
                            }

                        else:
                            self.data['results'] = { 
                                'user_id': existing['user_id'],
                            }
                            self.data['message'] = '중복 로그인이 감지되었습니다.'
                            self.status_code = 401

                    else:
                        self.status_code = 404
                        self.data['message'] = '존재하지 않는 유저입니다'
                  
        except Exception as e:
            logger.error(f"Error get user info: {e}")
            self.status_code = 500
            self.message = '서버 오류입니다.'
            pass
        

    # 중복 체크할 때 타는 api / user_id 받아서 유저정보 리턴
    async def chk_duplcate(self, user):
        conn = self._get_connection()

        try:
            if conn:
                with conn.cursor() as cursor:
                    check_sql = "SELECT * FROM session WHERE user_id = %s"
                    cursor.execute(check_sql, (user,))
                    existing = cursor.fetchone()

                    if existing:
                        if existing['session_id']:
                            self.data['results'] = { 'session_id': existing['session_id'] }
                        
                        else:
                            self.data['results'] = { 'session_id': existing['session_id'] }
                            self.data['message'] = '중복 로그인이 감지되었습니다.'
                            self.status_code = 401

                    else:
                        self.status_code = 404
                        self.data['message'] = '존재하지 않는 유저입니다'
                  
        except Exception as e:
            logger.error(f"Error duplicate user: {e}")
            self.status_code = 500
            self.message = '서버 오류입니다.'
            pass
    