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
async def api_select(user_no: int,symbol:str=''):

    
    """

    
    """
    mysql=MySQLAdapter()
    
    try:    
        mysql.get_position_list(user_no,symbol)
            


    except Exception as e:
        print(e)


    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)



@router.get('/get_order_list', summary='ORDER HISTORY', tags=['USER API'])
async def api_select(user_no: int):

    
    """

    
    """
    mysql=MySQLAdapter()
    
    try:    
        mysql.get_order_list(user_no)
            


    except Exception as e:
        print(e)


    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)


@router.get('/get_openorder_list', summary='OPEN ORDER', tags=['USER API'])
async def api_select(user_no: int,symbol:str =''):

    
    """

    
    """
    mysql=MySQLAdapter()
    
    try:    
        mysql.get_openorder_list(user_no,symbol)
            


    except Exception as e:
        print(e)


    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)



@router.get('/get_userbalance_list', summary='USER BALANCE', tags=['USER API'])
async def api_select(user_no: int):

    
    """

    
    """
    mysql=MySQLAdapter()
    
    try:    
        mysql.get_userbalance_list(user_no)
            


    except Exception as e:
        print(e)


    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)














