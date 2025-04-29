from fastapi import APIRouter, FastAPI, HTTPException, Header, Request
from logging.handlers import RotatingFileHandler 
from fastapi.responses import RedirectResponse
from fastapi.responses import JSONResponse, Response
from utils.auth import UserSession
from typing import Optional
from datetime import date
import logging
from pydantic import BaseModel
from datetime import date

router= APIRouter()

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


class SessionRequest(BaseModel):
    session_id: str
    user_id: str
    total_package: int
    end_date: date


@router.post('/save', summary='SESSION', tags=['SESSION API'])
async def save_user(data: SessionRequest):
    """
    유저 진입시 타는 api
    """
    mysql=UserSession()

    logger.info(f"session_id: {data.session_id}  user_id: {data.user_id}  total_package: {data.total_package}  end_date: {data.end_date}")
    
    try:    
        user_info = mysql.save_user(data.session_id, data.user_id, data.total_package, data.end_date)
        logger.info(f"user_info: {user_info}")
       
        response = JSONResponse(
            content={
                "redirect_url": "https://dominance.tryex.xyz/trading",
                "status": mysql.status_code,
            },
            status_code=mysql.status_code
        )


        # for key, value in user_info.items():
        logger.info(f"user_id: {user_info['user_id']}")

        response.set_cookie(
            key='user_id',
            value=user_info['user_id'],
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




@router.post('/duplicate-login', summary='SESSION', tags=['SESSION API'])
async def duplicate_user(data: SessionRequest):
    """
    리트리 중복 로그인 감지시 타는 api
    """
    mysql=UserSession()

    logger.info(f"duplicate_id: {data.session_id}  user_id: {data.user_id}  total_package: {data.total_package}  end_date: {data.end_date}")
    
    try:    
        user_info = await mysql.chk_login(data.session_id, data.user_id, data.total_package, data.end_date)
            
    except Exception as e:
        logger.error(f"Error duplicate: {e}")       
    

    return JSONResponse(
        content={
            "status": mysql.status_code,
            "msg": mysql.message
        },
        status_code=mysql.status_code
    )


class ChkRequest(BaseModel):
    user_id: str

@router.post('/user-info', summary='SESSION', tags=['SESSION API'])
async def user_info(request: Request):
    """
    모의 트레이딩 페이지 최초 접속시 유저 정보 주는 api
    """
    mysql=UserSession()
    
    try:    
        user_id = request.cookies.get('user_id')
        logger.info(f"user_id: {user_id}")       
        user_info = await mysql.get_user_info(user_id)
            
    except Exception as e:
        logger.error(f"Error get user info router: {e}")       
    
    return JSONResponse(mysql.data, status_code=mysql.status_code)


@router.post('/duplicate-check', summary='SESSION', tags=['SESSION API'])
async def duplicate_check(data: ChkRequest):
    """
    모의 트레이딩 중복 로그인 체크시 타는 api
    """
    mysql=UserSession()
    
    try:    
        user_info = await mysql.chk_duplcate(data.user_id)
            
    except Exception as e:
        logger.error(f"Error duplicate check: {e}")       
    
    return JSONResponse(mysql.data, status_code=mysql.status_code)

