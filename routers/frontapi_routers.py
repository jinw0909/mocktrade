from fastapi import APIRouter
from starlette.responses import JSONResponse
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from utils.frontapi import MySQLAdapter,MakeErrorType
from fastapi import APIRouter, HTTPException, UploadFile, File
from typing import Optional
router= APIRouter()


@router.get('/user_info', summary='USER INFO', tags=['USER API'])
async def api_select(retri_id:str ):

    
    """

    
    """
    mysql=MySQLAdapter()
    
    try:    
        mysql.get_user_info(retri_id)
            


    except Exception as e:
        print(e)


    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)




@router.post('/start_trading', summary='START', tags=['USER API'])
async def api_select(retri_id:str ):

    
    """

    
    """
    mysql=MySQLAdapter()
    
    try:    
        mysql.start_user(retri_id)
            


    except Exception as e:
        print(e)


    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)



@router.post('/charge_seed', summary='SEED CHARGE', tags=['USER API'])
async def api_select(retri_id: str,seed:float):

    
    """

    
    """
    mysql=MySQLAdapter()
    check = MakeErrorType()
    try:
        
         
        mysql.get_resetseed(retri_id,seed)
            
 


    except Exception as e:
        print(e)


    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)



@router.post('/reset_user', summary='RESET USER', tags=['USER API'])
async def api_select(retri_id: str):

    
    """

    
    """
    mysql=MySQLAdapter()
    print(retri_id)
    try:  
        mysql.reset_user(retri_id)
            


    except Exception as e:
        print(e)


    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)



@router.get('/get_position_list', summary='POSITION', tags=['USER API'])
async def api_select(user_no: str,symbol:str=''):

    
    """

    
    """
    mysql=MySQLAdapter()
    check = MakeErrorType()
    try:
        data=mysql.get_check_user(user_no)
        if len(data)>0  :
          
            user_id=data['id'].iloc[0]    
            mysql.get_position_list(user_id,symbol)
            
        else:
            
            mysql.return_dict_data['reCode']=105
            mysql.return_dict_data['message'] = check.error(mysql.return_dict_data['reCode'])
            mysql.status_code=423
            


    except Exception as e:
        print(e)


    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)



@router.get('/get_order_list', summary='ORDER HISTORY', tags=['USER API'])
async def api_select(user_no: str):

    
    """

    
    """
    mysql=MySQLAdapter()
    check = MakeErrorType()
    try:
        data=mysql.get_check_user(user_no)
        if len(data)>0  :
          
            user_id=data['id'].iloc[0]     
            mysql.get_order_list(user_id)
            
        else:
            
            mysql.return_dict_data['reCode']=105
            mysql.return_dict_data['message'] = check.error(mysql.return_dict_data['reCode'])
            mysql.status_code=423
            

    except Exception as e:
        print(e)


    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)


@router.get('/get_openorder_list', summary='OPEN ORDER', tags=['USER API'])
async def api_select(user_no: str,symbol:str =''):

    
    """

    
    """
    mysql=MySQLAdapter()
    check = MakeErrorType()
    try:
        data=mysql.get_check_user(user_no)
        if len(data)>0  :
          
            user_id=data['id'].iloc[0] 
            mysql.get_openorder_list(user_id,symbol)
            
        else:
            
            mysql.return_dict_data['reCode']=105
            mysql.return_dict_data['message'] = check.error(mysql.return_dict_data['reCode'])
            mysql.status_code=423

    except Exception as e:
        print(e)


    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)



@router.get('/get_position_history', summary='POSITION HISTORY', tags=['USER API'])
async def api_select(user_no: str):

    
    """

    
    """
    mysql=MySQLAdapter()
    check = MakeErrorType()
    try:
        data=mysql.get_check_user(user_no)
        if len(data)>0  :
          
            user_id=data['id'].iloc[0] 
            mysql.get_position_history(user_id)
            
        else:
            
            mysql.return_dict_data['reCode']=105
            mysql.return_dict_data['message'] = check.error(mysql.return_dict_data['reCode'])
            mysql.status_code=423

    except Exception as e:
        print(e)


    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)




@router.get('/get_userbalance_list', summary='USER BALANCE', tags=['USER API'])
async def api_select(user_no: str):

    
    """

    
    """
    mysql=MySQLAdapter()
    check = MakeErrorType()
    try:  
        data=mysql.get_check_user(user_no)
        if len(data)>0  :
          
            user_id=data['id'].iloc[0]   
            mysql.get_userbalance_list(user_id)
        else:
            
            mysql.return_dict_data['reCode']=105
            mysql.return_dict_data['message'] = check.error(mysql.return_dict_data['reCode'])
            mysql.status_code=423   


    except Exception as e:
        print(e)


    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)




@router.get('/get_price', summary='USER PRICE', tags=['USER API'])
async def api_select(symbol: str):

    
    """

    
    """
    mysql=MySQLAdapter()
    # check = MakeErrorType()
    try:  
        
        mysql.get_price(symbol)
        

    except Exception as e:
        print(e)


    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)














