import os
import sys
import pymysql.cursors
import base64
import json
from Crypto.Cipher import AES
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
from datetime import datetime
from Crypto.Util.Padding import pad, unpad

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

# 테스트 로그인 파일 핸들러 생성
file_handler_test = RotatingFileHandler("./logs/auth/test.log", maxBytes=5*1024*1024, backupCount=3)
file_handler_test.setLevel(logging.INFO)
file_handler_test.setFormatter(log_formatter)

# 테스트 로그인 파일 핸들러 생성
logger_test = logging.getLogger(__name__)
logger_test.setLevel(logging.INFO)
logger_test.addHandler(file_handler_test)

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

    # key, iv 를 받아 암호화/복호화 객체 생성
    def _get_cipher(self, key: str, iv: str) -> AES:
        return AES.new(key.encode('utf-8'), AES.MODE_CBC, iv.encode('utf-8'))
    
    # 암호화 함수
    def encrypt(self, key: str, iv: str, data: str) -> str:
        try:
            json_data = json.dumps(data)
            raw_data = pad(json_data.encode('utf-8'), AES.block_size)
            cipher = self._get_cipher(key, iv)
            encrypted_data = cipher.encrypt(raw_data)
            return base64.b64encode(encrypted_data).decode('utf-8')
        except (ValueError, UnicodeDecodeError) as e:
            raise ValueError("암호화 중 오류가 발생했습니다.") from e

    # 복호화 함수
    def decrypt(self, encrypted_data: str, key: str, iv: str) -> str:
        try:
            encrypted_data = base64.b64decode(encrypted_data)
            cipher = self._get_cipher(key, iv)
            decrypted_data = cipher.decrypt(encrypted_data)
            return unpad(decrypted_data, AES.block_size).decode('utf-8')
        except (ValueError, UnicodeDecodeError) as e:
            raise ValueError("복호화 중 오류가 발생했습니다.") from e
        

    # 처음 페이지 이동시 타는 api / 유저정보 DB 저장    
    def save_user(self, data):
        conn = self._get_connection()

        user_info = {
            'session_id': data.session_id,
            'user_id': data.user_id,
            'pakage': data.total_package,
            'end_date': data.end_date,
            'user_lang': data.user_lang
        }

        try:
            if conn:
                with conn.cursor() as cursor:
                    check_sql = "SELECT * FROM session WHERE user_id = %s"
                    cursor.execute(check_sql, (data.user_id,))
                    existing = cursor.fetchone()
                    values = (data.session_id, data.total_package, data.end_date, data.user_lang , data.user_id)
                    logger.info(f"values: {values}")

                    if existing:
                        update_sql = """
                            UPDATE session
                            SET session_id = %s, total_package = %s, end_date = %s, user_lang = %s
                            WHERE user_id = %s
                        """
                        cursor.execute(update_sql, values)

                    else: 
                        insert_sql  = """
                            INSERT INTO session (session_id, total_package, end_date, user_lang, user_id)
                            VALUES (%s, %s, %s, %s, %s)
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

    
    # 리트리 중복 로그인시 타는 api / session 값 이후 유저로 업데이트
    async def chk_login(self, session, user):
        conn = self._get_connection()

        user_info = {
            'session_id': session,
            'user_id': user
        }

        try:
            if conn:
                with conn.cursor() as cursor:
                    check_sql = "SELECT * FROM session WHERE user_id = %s"
                    cursor.execute(check_sql, (user,))
                    existing = cursor.fetchone()
                    values = (session, pakage, end_date, user)

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
        logger_test.info(f"user: {user}")
        conn = self._get_connection()

        try:
            if conn:
                with conn.cursor() as cursor:
                    check_sql = "SELECT * FROM session WHERE user_id = %s"
                    cursor.execute(check_sql, (user['user_id'],))
                    existing = cursor.fetchone()
                    logger_test.info(f"existing: {existing}")

                    if existing:
                        if existing['session_id'] == user['session_id']:
                            self.data['results'] = { 
                                'user_id': existing['user_id'],
                                'session_id': existing['session_id'],
                                'package': existing['total_package'],
                                'end_date': existing['end_date'].isoformat() if isinstance(existing['end_date'], datetime) else existing['end_date'],
                                'user_lang': existing['user_lang']
                            }
                        else:
                            logger.info(f"else")
                            self.data['results'] = { 
                                'user_id': existing['user_id'],
                            }
                            self.data['message'] = '중복 로그인이 감지되었습니다.'
                            self.status_code = 401

                    else:
                        self.status_code = 404
                        self.data['message'] = '존재하지 않는 유저입니다'
                    
                        
                    logger_test.info(f"{self.data['results']}")
                    
                  
        except Exception as e:
            logger_test.error(f"Error get user info: {e}")
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
                    cursor.execute(check_sql, (user.user_id,))
                    existing = cursor.fetchone()

                    if existing:
                        if existing['session_id'] == user.session_id:
                            self.data['results'] = { 'session_id': existing['session_id'] }
                            self.data['message'] = ''
                        
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
    