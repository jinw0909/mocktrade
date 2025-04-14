from fastapi import APIRouter
from starlette.responses import JSONResponse
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from utils.settings import MySQLAdapter
from fastapi import APIRouter, HTTPException, UploadFile, File
from typing import Optional
router= APIRouter()

@router.get('/test', summary = 'test router', tags = ['SETTINGS API'])
async def api_test():
    # '''
    
    # ''' 
    # mysql = MYSQLAdapter()
    
    # try:
    #     mysql.get_signal()
    # except Exception as e:
    #     print(e)
    
    return JSONResponse(content = {"message": "Hello Test"}, status_code = 200) 
    

@router.post('/tpsl', summary = 'set tp/sl of a single order', tags = ['SETTINGS API'])
async def api_tpsl(order_no: int, user_no: int, tp: float, sl: float):
     
    mysql = MYSQLAdapter()
    responseMessage = ''

    try:
        userId = mysql.get_userId(order_no)
        if userId == 0 or userId != user_no:
            print('Invalid Request')
            responseMessage = "Invalid Request"
        else:
            updateResult = mysql.set_tpsl(order_no, tp, sl)
            print(updateResult)
            responseMessage = updateResult

    except Exception as e:
        print(e)
        responseMessage = f"Exception occurred: {str(e)}"
    
    return JSONResponse(content = {"message": responseMessage}, status_code = 200) 

    
@router.post('/reload', summary = 'reload test', tags = ["SETTINGS API"])
async def api_realod():
    return JSONResponse(content = {"message": "reload test"}, status_Code = 200)
