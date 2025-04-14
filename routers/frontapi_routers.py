from fastapi import APIRouter
from starlette.responses import JSONResponse
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from utils.frontapi import MySQLAdapter
from fastapi import APIRouter, HTTPException, UploadFile, File
from typing import Optional
router= APIRouter()






@router.get('/get_position_list', summary='POSITION', tags=['USER API'])
async def api_select(user_no: int, symbol: str, margin_type: int, leverage: int,price:float, usdt: Optional[float] = 0, amount: Optional[int] = 0):

    
    """

    
    """
    mysql=MySQLAdapter()
    
    try:    
        mysql.buy_limit_order(user_no,symbol,margin_type,leverage,price,usdt,amount)
            


    except Exception as e:
        print(e)


    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)










