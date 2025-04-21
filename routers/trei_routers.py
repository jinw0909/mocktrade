from fastapi import APIRouter
from starlette.responses import JSONResponse
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from utils.trei import MySQLAdapter,MakeErrorType
from fastapi import APIRouter, HTTPException, UploadFile, File
from typing import Optional
router= APIRouter()



# @router.get('/btc-signal', summary='SIGNAL', tags=['SIGNAL API'])
# async def api_select():

    
#     """

   
#     """
#     mysql=MySQLAdapter()
    
#     try:    
#         mysql.get_signal()
            


#     except Exception as e:
#         print(e)
        

#     return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)



@router.post('/buy_limit_order', summary='ORDER', tags=['ORDER API'])
async def api_select(user_no: str, symbol: str, margin_type: int, leverage: int,price:float, usdt: Optional[float] = 0, amount: Optional[float] = 0,tp: Optional[float] = 0, sl: Optional[float] = 0):

    
    """

    
    """
    mysql=MySQLAdapter()
    check = MakeErrorType()
    try:
        data=mysql.get_check_user(user_no)
        # print('asdasdsadas',data)
        if len(data)>0  :
          
            user_id=data['id'].iloc[0]
            mysql.buy_limit_order(user_id,symbol,margin_type,leverage,price,usdt,amount,tp,sl)
            mysql.insert_trade_log(user_id, symbol, 'limit', margin_type, 'buy', price, usdt, amount, leverage, tp,sl,mysql.return_dict_data['reCode'],mysql.return_dict_data['message'])    
        else:
            
            mysql.return_dict_data['reCode']=105
            mysql.return_dict_data['message'] = check.error(mysql.return_dict_data['reCode'])
            mysql.status_code=423
            mysql.insert_trade_log(user_no, symbol, 'limit', margin_type, 'buy', price, usdt, amount, leverage, tp,sl,mysql.return_dict_data['reCode'],mysql.return_dict_data['message'])    
            

    except Exception as e:
        print(e)


    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)




@router.post('/sell_limit_order', summary='ORDER', tags=['ORDER API'])
async def api_select(user_no: str, symbol: str, margin_type: int, leverage: int,price:float, usdt: Optional[float] = 0, amount: Optional[float] = 0,tp: Optional[float] = 0, sl: Optional[float] = 0):

    
    """
    
   
    """
    mysql=MySQLAdapter()
    check = MakeErrorType()
    try:
        data=mysql.get_check_user(user_no)
        
        if len(data)>0  :
          
            user_id=data['id'].iloc[0]
            mysql.sell_limit_order(user_id,symbol,margin_type,leverage,price,usdt,amount,tp,sl)
            mysql.insert_trade_log(user_id, symbol, 'limit', margin_type, 'sell', price, usdt, amount, leverage, tp,sl,mysql.return_dict_data['reCode'],mysql.return_dict_data['message']) 
        else:
            
            mysql.return_dict_data['reCode']=105
            mysql.return_dict_data['message'] = check.error(mysql.return_dict_data['reCode'])
            mysql.status_code=423
            mysql.insert_trade_log(user_no, symbol, 'limit', margin_type, 'sell', price, usdt, amount, leverage, tp,sl,mysql.return_dict_data['reCode'],mysql.return_dict_data['message'])
    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)



@router.post('/buy_market_order', summary='ORDER', tags=['ORDER API'])
async def api_select(user_no: str, symbol: str, margin_type: int, leverage: int, usdt: Optional[float] = 0, amount: Optional[float] = 0,tp: Optional[float] = 0, sl: Optional[float] = 0):

    
    """

   
    """
    mysql=MySQLAdapter()
    check = MakeErrorType()
    try:
        data=mysql.get_check_user(user_no)
        if len(data)>0  :
          
            user_id=data['id'].iloc[0]
            mysql.buy_market_order(user_id,symbol,margin_type,leverage,usdt,amount,tp,sl)
            mysql.insert_trade_log(user_id, symbol, 'market', margin_type, 'buy', mysql.price1, usdt, amount, leverage, tp,sl,mysql.return_dict_data['reCode'],mysql.return_dict_data['message'])    
        else:
            
            mysql.return_dict_data['reCode']=105
            mysql.return_dict_data['message'] = check.error(mysql.return_dict_data['reCode'])
            mysql.status_code=423
            mysql.insert_trade_log(user_no, symbol, 'market', margin_type, 'buy', mysql.price1, usdt, amount, leverage, tp,sl,mysql.return_dict_data['reCode'],mysql.return_dict_data['message']) 
    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)




@router.post('/sell_market_order', summary='ORDER', tags=['ORDER API'])
async def api_select(user_no: str, symbol: str, margin_type: int, leverage: int, usdt: Optional[float] = 0, amount: Optional[float] = 0,tp: Optional[float] = 0, sl: Optional[float] = 0):

    
    """

   
    """
    mysql=MySQLAdapter()
    check = MakeErrorType()
    try:
        data=mysql.get_check_user(user_no)
        if len(data)>0  :
          
            user_id=data['id'].iloc[0]   
            mysql.sell_market_order(user_id,symbol,margin_type,leverage,usdt,amount,tp,sl)
            mysql.insert_trade_log(user_id, symbol, 'market', margin_type, 'sell', mysql.price1, usdt, amount, leverage, tp,sl,mysql.return_dict_data['reCode'],mysql.return_dict_data['message'])  
        else:
            
            mysql.return_dict_data['reCode']=105
            mysql.return_dict_data['message'] = check.error(mysql.return_dict_data['reCode'])
            mysql.status_code=423
            mysql.insert_trade_log(user_no, symbol, 'market', margin_type, 'sell', mysql.price1, usdt, amount, leverage, tp,sl,mysql.return_dict_data['reCode'],mysql.return_dict_data['message'])
    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)



@router.post('/cancle_order', summary='ORDER', tags=['ORDER API'])
async def api_select(user_no: str,ordid:int):

    
    """

   
    """
    mysql=MySQLAdapter()
    check = MakeErrorType()
    try:
        data=mysql.get_check_user(user_no)
        # print(data)
        if len(data)>0  :
            # print('asdasdasdasdsa')
            user_id=data['id'].iloc[0] 
            mysql.cancel_order(user_id,ordid)
        else:
            
            mysql.return_dict_data['reCode']=105
            mysql.return_dict_data['message'] = check.error(mysql.return_dict_data['reCode'])
            mysql.status_code=423


    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)



@router.post('/close_position', summary='CLOSE POSITION', tags=['ORDER API'])
async def api_select(user_no: str,position_id:int):

    
    """

   
    """
    mysql=MySQLAdapter()
    check = MakeErrorType()
    try:
        data=mysql.get_check_user(user_no)
        
        if len(data)>0  :
            
            user_id=data['id'].iloc[0] 
            mysql.cancel_position(user_id,position_id)
        else:
            
            mysql.return_dict_data['reCode']=105
            mysql.return_dict_data['message'] = check.error(mysql.return_dict_data['reCode'])
            mysql.status_code=423 


    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)




@router.post('/position_tp_sl', summary='POSITION TP SL', tags=['ORDER API'])
async def api_select(user_no: str,position_id:int,tp:float=0,sl:float=0):

    
    """

   
    """
    mysql=MySQLAdapter()
    check = MakeErrorType()
    try:  
        data=mysql.get_check_user(user_no)  
        if len(data)>0  :
            
            user_id=data['id'].iloc[0] 
            mysql.update_tpsl_position(user_id,position_id,tp,sl)
            
        else:
            
            mysql.return_dict_data['reCode']=105
            mysql.return_dict_data['message'] = check.error(mysql.return_dict_data['reCode'])
            mysql.status_code=423 

    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)



