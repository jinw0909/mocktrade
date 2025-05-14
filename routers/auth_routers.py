from fastapi import APIRouter, FastAPI, HTTPException, Header, Request
from logging.handlers import RotatingFileHandler 
from fastapi.responses import RedirectResponse
from fastapi.responses import JSONResponse, Response
from utils.auth import UserSession
from typing import Optional
from datetime import date
import logging
from pydantic import BaseModel
from dotenv import load_dotenv
import os
import json

load_dotenv()
router= APIRouter()

encryption_key = os.getenv('ENCRYPTION_KEY')
encryption_iv = os.getenv('ENCRYPTION_IV')

# 로그 포맷 정의
log_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# 파일 핸들러 생성
file_handler = RotatingFileHandler("./logs/auth/user_info.log", maxBytes=5*1024*1024, backupCount=3)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(log_formatter)

# 기본 로거 설정
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)


# 중복 로그인 파일 핸들러 생성
file_handler_duplicated = RotatingFileHandler("./logs/auth/duplicated.log", maxBytes=5*1024*1024, backupCount=3)
file_handler_duplicated.setLevel(logging.INFO)
file_handler_duplicated.setFormatter(log_formatter)

# 중복 로그인 기본 로거 설정
logger_duplicated = logging.getLogger(__name__)
logger_duplicated.setLevel(logging.INFO)
logger_duplicated.addHandler(file_handler_duplicated)


# 테스트 로그인 파일 핸들러 생성
file_handler_test = RotatingFileHandler("./logs/auth/test.log", maxBytes=5*1024*1024, backupCount=3)
file_handler_test.setLevel(logging.INFO)
file_handler_test.setFormatter(log_formatter)

# 테스트 로그인 파일 핸들러 생성
logger_test = logging.getLogger(__name__)
logger_test.setLevel(logging.INFO)
logger_test.addHandler(file_handler_test)


class SessionRequest(BaseModel):
    session_id: str
    user_id: str
    total_package: int
    end_date: date
    user_lang: str


@router.post('/save', summary='SESSION', tags=['SESSION API'])
async def save_user(data: SessionRequest):
    """
    유저 진입시 타는 api
    """
    mysql=UserSession()

    logger.info(f"################### session_id: {data.session_id}  user_id: {data.user_id}  total_package: {data.total_package}  end_date: {data.end_date} user_lang': {data.user_lang}")
    
    try:    
        user_info = mysql.save_user(data)
        logger.info(f"user_info: {user_info}")

        user_data = {
            'user_id': user_info['user_id'],
            'session_id': user_info['session_id']
        }
        
        encrypt_data = mysql.encrypt(encryption_key, encryption_iv, user_data)
        logger.info(f"encrypt_data: {encrypt_data}")

        response = JSONResponse(
            content={
                "redirect_url": "https://dominance.tryex.xyz/trading-chart",
                "status": mysql.status_code,
            },
            status_code=mysql.status_code
        )

        response.set_cookie(
            key='encrypt_data',
            value=encrypt_data,
            samesite="None",
            secure=True, 
            httponly=True,
            domain=".tryex.xyz",
        )

        logger.info(f"headers: {response.headers}")

        return response
            
    except Exception as e:
        logger.error(f"Error save: {e}")
        return JSONResponse(
            content={
                "status": mysql.status_code,
                "msg": mysql.message
            },
            status_code=mysql.status_code
        )


class duplicateRequest(BaseModel):
    session_id: str
    user_id: str
    total_package: int
    end_date: Optional[date] = None

@router.post('/duplicate-login', summary='SESSION', tags=['SESSION API'])
async def duplicate_user(request: Request, data: duplicateRequest):
    """
    리트리 중복 로그인 감지시 타는 api
    """
    mysql=UserSession()

    if data.session_id == '':
        logger_duplicated.info(f"========= 중복로그인 api 실행 빈값인 경우 =========")
        logger_duplicated.info(f"data: {data}")
        logger_duplicated.info(f"==================")

        return JSONResponse(
            content={
                "status": 200,
                "msg": "로그인 후 이용해주세요."
            },
            status_code=200
        )

    else:
        logger_duplicated.info(f"========= 중복로그인 api 실행 =========")
        logger_duplicated.info(f"duplicate_id: {data.session_id}  user_id: {data.user_id}  total_package: {data.total_package}  end_date: {data.end_date}")
        logger_duplicated.info(f"headers: {request.headers}")

        x_forwarded_for = request.headers.get("X-Forwarded-For")

        if x_forwarded_for:
            client_ip = x_forwarded_for.split(",")[0].strip()
        else:
            client_ip = request.scope.get("client")[0] if request.scope.get("client") else "unknown"

        logger_duplicated.info(
            f"IP: {client_ip} | duplicate_id: {data.session_id} | user_id: {data.user_id} | "
            f"total_package: {data.total_package} | end_date: {data.end_date}"
        )
        try:    
            user_info = await mysql.chk_login(data.session_id, data.user_id, data.total_package, data.end_date)
                
        except Exception as e:
            logger_duplicated.error(f"Error duplicate: {e}")       
        
        logger_duplicated.info(f"========= 중복로그인 api 종료 =========")

        return JSONResponse(
            content={
                "status": mysql.status_code,
                "msg": mysql.message
            },
            status_code=mysql.status_code
        )


@router.post('/user-info', summary='SESSION', tags=['SESSION API'])
async def user_info(request: Request):
    """
    모의 트레이딩 페이지 최초 접속시 유저 정보 주는 api
    """
    try:    
        mysql=UserSession() 

        encrypt_data = request.cookies.get('encrypt_data')
        logger_test.info(f"request encrypt_data: {encrypt_data}")

        decrypt_data = mysql.decrypt(encrypt_data, encryption_key, encryption_iv)
        parsed_decrypt = json.loads(decrypt_data)
        logger_test.info(f"decrypt_data: {parsed_decrypt}")
       
        await mysql.get_user_info(parsed_decrypt)
        
        return JSONResponse(mysql.data, status_code=mysql.status_code)

        # data = {
        #     "results": {
        #         "user_id": "re-2",
        #         "session_id": "631212824",
        #         "package": "10000",
        #         "end_date": "2025-07-30",
        #         "user_lang": "KR"
        #     },
        #     "message": ""
        # }

        # return JSONResponse(data, status_code=200)

            
    except Exception as e:
        logger_test.error(f"Error get user info router: {e}")       


   


class ChkRequest(BaseModel):
    user_id: str
    session_id: str

@router.post('/duplicate-check', summary='SESSION', tags=['SESSION API'])
async def duplicate_check(data: ChkRequest):
    """
    모의 트레이딩 중복 로그인 체크시 타는 api
    """
    mysql=UserSession()
    
    try:    
        user_info = await mysql.chk_duplcate(data)
            
    except Exception as e:
        logger.error(f"Error duplicate check: {e}")       
    
    return JSONResponse(mysql.data, status_code=mysql.status_code)



# @router.get("/check_ip", summary='IP CHECK', tags=['IP CHECK API'])
#     access_ips = ['121.133.55.203', '203.0.113.45']

#     def ip_restriction(request: Request):
#     """
#     ip 제한하는 api
#     """
#     user_ip = request.client.host

#     if user_ip in access_ips:
#         raise HTTPException(status_code=HTTP_403_FORBIDDEN, detail="접근이 제한된 IP입니다.")
#     return user_ip