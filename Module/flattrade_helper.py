from urllib.parse import parse_qs, urlparse
from NorenRestApiPy.NorenApi import NorenApi
from Module.generic_helper import get_sqlite_db_connection, get_sqlite_db_connection_np, get_expiry_date

import pandas as pd
import hashlib
import requests
import pyotp
import json


# Set all the FlatTrade Specific Variables
refererUrl = 'https://auth.flattrade.in/'
sessionUrl = 'https://authapi.flattrade.in/auth/session'
authUrl = 'https://authapi.flattrade.in/ftauth'
tokenUrl = 'https://authapi.flattrade.in/trade/apitoken'
hostUrl = 'https://piconnect.flattrade.in/PiConnectTP/'
websocketUrl = 'wss://piconnect.flattrade.in/PiConnectWSTp/'


class FlatTradeApiPy(NorenApi):
    def __init__(self):
        NorenApi.__init__(self, host=hostUrl,
                          websocket=websocketUrl)


def ft_get_api(config):
    print(config['tokenfile'])
    filePath = f"Config/{config['tokenfile']}"
    userName = config['username']
    password = config['password']

    try:
        f = open(filePath)
        data = json.load(f)
        # print(data)
        token = data['token']
        ftApi = FlatTradeApiPy()

        ret = ftApi.set_session(userid=userName, password=password,
                              usertoken=token)

        ret = ftApi.get_limits()
        print(ret)
        # status = json.loads(res)
        if ret['stat'] != 'Ok':
            return ft_login(config)

        return ftApi

    except Exception as err:
        print(' Exception From FlatTrade API Object Creation with Existing Token')
        print(f"Unexpected {err=}, {type(err)=}")
        return ft_login(config)


def ft_login(config):

    filePath = f"Config/{config['tokenfile']}"
    userName = config['username']
    password = config['password']
    totp = config['totpkey']
    apiKey = config['apikey']
    apiSec = config['apisecret']

    headerJson = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
                  "referer": refererUrl}

    passwordEncrpted = hashlib.sha256(password.encode()).hexdigest()
    session = requests.Session()
    try:
        res_sid = session.post(sessionUrl, headers=headerJson)
        sid = res_sid.text
        # print(f'sid {sid}')

        totp = pyotp.TOTP(totp).now()
        payload = {"UserName": userName, "Password": passwordEncrpted, "PAN_DOB": totp,
                   "App": "", "ClientID": "", "Key": "", "APIKey": apiKey, "sid": sid}

        res_code = session.post(authUrl, json=payload)

        reqcodeRes = res_code.json()
        # print(reqcodeRes)
        parsed = urlparse(reqcodeRes['RedirectURL'])
        reqCode = parse_qs(parsed.query)['code'][0]
        # print(reqCode)

        api_secret = apiKey + reqCode + apiSec
        api_secret = hashlib.sha256(api_secret.encode()).hexdigest()

        payload = {"api_key": apiKey,
                   "request_code": reqCode, "api_secret": api_secret}

        res_token = session.post(tokenUrl, json=payload)
        token = res_token.json()['token']

        f = open(filePath, "w")
        f.write('{"token": "%s"}' % token)
        f.close()

        ftApi = FlatTradeApiPy()

        ret = ftApi.set_session(
            userid=userName, password=password, usertoken=token)

        ret = ftApi.get_limits()
        print(ret)

        return ftApi
    except Exception as err:
        print(' Exception From FlatTrade API Object Creation')
        print(f"Unexpected {err=}, {type(err)=}")
        raise


def ft_update_scripts(config):
    try:
        db_conn = get_sqlite_db_connection_np(config=config)
        nse_columns = ['Exchange','Token','LotSize','Symbol','TradingSymbol','Instrument','TickSize'] # or [0,1,2,3]
        nseDF = pd.read_csv('https://api.shoonya.com/NSE_symbols.txt.zip',usecols= nse_columns, storage_options={"User-Agent":"pandas"})

        nfo_columns = ['Exchange','Token','LotSize','Symbol','TradingSymbol','Expiry','Instrument','OptionType','StrikePrice','TickSize']
        nfoDF = pd.read_csv('https://api.shoonya.com/NFO_symbols.txt.zip',usecols= nfo_columns, storage_options={"User-Agent":"pandas"})
        
        # print(nseDF)
        # print(nfoDF)

        nseDF.to_sql("script_symbols_stage", db_conn, if_exists='append')
        db_conn.commit()

        nfoDF.to_sql("script_symbols_stage", db_conn, if_exists='append')
        db_conn.commit()    


        db_crsr = db_conn.cursor()

        # another SQL command to insert the data in the table
        sql_command = """   UPDATE 	script_symbols
                            SET 	Exchange = script_symbols_stage.Exchange,
                                    LotSize = script_symbols_stage.LotSize,
                                    Symbol = script_symbols_stage.Symbol,
                                    TradingSymbol = script_symbols_stage.TradingSymbol,
                                    Instrument = script_symbols_stage.Instrument,
                                    TickSize = script_symbols_stage.TickSize,
                                    Expiry = script_symbols_stage.Expiry,
                                    OptionType = script_symbols_stage.OptionType,
                                    StrikePrice = script_symbols_stage.StrikePrice
                            FROM 	script_symbols_stage
                            WHERE 	script_symbols.Token = script_symbols_stage.Token;"""

        db_crsr.execute(sql_command)
        db_conn.commit()

        sql_command = """   INSERT OR IGNORE INTO script_symbols
                                (	"Exchange","Token","LotSize","Symbol","TradingSymbol",
                                    "Instrument","TickSize","Expiry","OptionType","StrikePrice")
                            SELECT 	"Exchange","Token","LotSize","Symbol","TradingSymbol",
                                    "Instrument","TickSize","Expiry","OptionType","StrikePrice"
                            FROM 	script_symbols_stage
                            ORDER BY script_symbols_stage.[index];"""
        db_crsr.execute(sql_command)
        db_conn.commit()

        sql_command = " DELETE FROM script_symbols_stage;"
        db_crsr.execute(sql_command)
        db_conn.commit()

        db_crsr.close()
        db_conn.close()
        print('Updated FlatTrade Scripts.')        
    
    except Exception as err:
        print(' Exception From sh_update_scripts')
        print(f"Unexpected {err=}, {type(err)=}")

def ft_get_script_token(config,script_name):
    try:

        sqlite_conn = get_sqlite_db_connection(config)
        sqlite_crsr = sqlite_conn.cursor()

        sql_command = f"  SELECT Exchange, cast(Token as text) Token FROM script_symbols  where TradingSymbol = '{script_name}' limit 1;"
        sqlite_crsr.execute(sql_command)
        sqlite_conn.commit()

        for token in sqlite_crsr:
            # print(token)
            script_exchange = token[0]
            script_token = token[1]

        sqlite_crsr.close()
        sqlite_conn.close()

        return script_exchange, script_token

    except Exception as err:
        print(' Exception From ft_get_script_token')
        print(f"Unexpected {err=}, {type(err)=}")


def ft_get_option_tokens(config,ftApi,index_name,index_symbol,option_level,option_strike_diff):
    try:
        expiryDate = get_expiry_date()
        exchange, token = ft_get_script_token(config,index_name)
        token_str = f"{exchange}|{token}"
        index_ltp = ftApi.get_quotes(exchange=exchange,token=token)['lp']

        base_value = int(float(index_ltp)//option_strike_diff)*option_strike_diff
        index_range_min = base_value
        index_range_max = index_range_min + option_strike_diff

        # min_option_call = index_symbol + expiryDate + 'C' + str(index_range_min)
        # min_option_put = index_symbol + expiryDate + 'P' + str(index_range_min)
        # max_option_call = index_symbol + expiryDate + 'C' + str(index_range_max)
        # max_option_put = index_symbol + expiryDate + 'P' + str(index_range_max)    
        # print(min_option_call, min_option_put,max_option_call,max_option_put)   

        level = 0
        while level < option_level:
            min_option_call = index_symbol + expiryDate + 'C' + str(index_range_min - level * option_strike_diff)
            min_option_put = index_symbol + expiryDate + 'P' + str(index_range_min - level * option_strike_diff)
            max_option_call = index_symbol + expiryDate + 'C' + str(index_range_max + level * option_strike_diff)
            max_option_put = index_symbol + expiryDate + 'P' + str(index_range_max + level * option_strike_diff)    

            exchange, token = ft_get_script_token(config,min_option_call)
            token_str += f"#{exchange}|{token}"
            exchange, token = ft_get_script_token(config,min_option_put)
            token_str += f"#{exchange}|{token}"   
            exchange, token = ft_get_script_token(config,max_option_call)
            token_str += f"#{exchange}|{token}"   
            exchange, token = ft_get_script_token(config,max_option_put)
            token_str += f"#{exchange}|{token}"                                    
            level += 1
        
        return token_str
    except Exception as err:
        print(' Exception From sh_get_option_tokens')
        print(f"Unexpected {err=}, {type(err)=}")

def ft_insert_tics(config,df):
    try:

        db_conn = get_sqlite_db_connection(config)

        df.to_sql("script_tick_stage", db_conn, if_exists='append')
        db_conn.commit()

        db_crsr = db_conn.cursor()

        # another SQL command to insert the data in the table
        sql_command = """   UPDATE 	script_tick
                            SET 	Exchange = script_tick_stage.e,
                                    TradingSymbol = script_tick_stage.ts,
                                    LotSize = script_tick_stage.ls,
                                    TickSize = script_tick_stage.ti,
                                    LastTradedPrice = script_tick_stage.lp,
                                    PercentageChange = script_tick_stage.pc,
                                    EpochTime = script_tick_stage.ft,
                                    OI = script_tick_stage.oi,
                                    PreviousOI = script_tick_stage.poi
                            FROM 	script_tick_stage
                            WHERE 	script_tick_stage.tk = script_tick.Token AND
                                    script_tick_stage.ft_ts = script_tick.TimeStamp;"""

        db_crsr.execute(sql_command)
        db_conn.commit()

        sql_command = """   INSERT OR IGNORE INTO script_tick
                                (	"Exchange","Token","TradingSymbol","LotSize","TickSize","LastTradedPrice",
                                    "PercentageChange","EpochTime","OI","PreviousOI","TimeStamp" )
                            SELECT	"e","tk","ts","ls","ti","lp","pc","ft","oi","poi","ft_ts"
                            FROM 	script_tick_stage
                            ORDER BY script_tick_stage.[index];"""
        db_crsr.execute(sql_command)
        db_conn.commit()

        sql_command = " DELETE FROM script_tick_stage;"
        db_crsr.execute(sql_command)
        db_conn.commit()

        db_crsr.close()
        db_conn.close()

    except Exception as err:
        print(' Exception From sh_insert_tics')
        print(f"Unexpected {err=}, {type(err)=}")

def ft_insert_tics_direct(config,df):
    try:

        db_conn = get_sqlite_db_connection(config)

        # df.to_sql("script_tick_stage", db_conn, if_exists='append')
        # db_conn.commit()
        # print(df['e'])
        db_crsr = db_conn.cursor()

        sql_command = f"""   INSERT OR IGNORE INTO script_tick
                                (	"Exchange","Token","TradingSymbol","LotSize","TickSize","LastTradedPrice",
                                    "PercentageChange","EpochTime","OI","PreviousOI","TimeStamp","Volume" )
                            VALUES ( '{df['e'][0]}','{df['tk'][0]}','{df['ts'][0]}','{df['ls'][0]}','{df['ti'][0]}','{df['lp'][0]}','{df['pc'][0]}','{df['ft'][0]}','{df['oi'][0]}','{df['poi'][0]}','{df['ft_ts'][0]}','{df['v'][0]}'); """


        db_crsr.execute(sql_command)
        db_conn.commit()

        db_crsr.close()
        db_conn.close()

    except Exception as err:
        print(' Exception From sh_insert_tics')
        print(f"Unexpected {err=}, {type(err)=}")


def ft_update_order(config,df):
    try:

        db_conn = get_sqlite_db_connection_np(config)

        df.to_sql("order_details_stage", db_conn, if_exists='append')
        db_conn.commit()

        db_crsr = db_conn.cursor()

        # another SQL command to insert the data in the table
        sql_command = """   UPDATE 	order_details
                            SET 	userid = order_details_stage.uid,
                                    scriptname = order_details_stage.tsym,
                                    quantity = order_details_stage.qty,
                                    trantype = order_details_stage.trantype,
                                    purchtype = order_details_stage.prctyp,
                                    token = order_details_stage.token,
                                    orderprice = order_details_stage.prcftr,
                                    status = order_details_stage.status,
                                    ordertime = order_details_stage.norentm,
                                    exchtime = order_details_stage.exch_tm,
                                    cancelqty = order_details_stage.cancelqty,
                                    avgprc = order_details_stage.avgprc,
                                    fillshares = order_details_stage.fillshares,
                                    trn_status = order_details_stage.stat,
                                    kidid = order_details_stage.kidid,
                                    actuserid = order_details_stage.actid,
                                    exchange = order_details_stage.exch,
                                    ordenttm = order_details_stage.ordenttm,
                                    retention = order_details_stage.ret,
                                    multiple = order_details_stage.mult,
                                    prcftr = order_details_stage.prc,
                                    dname = order_details_stage.dname,
                                    priceprc = order_details_stage.pp,
                                    lotsize = order_details_stage.ls,
                                    ticksize = order_details_stage.ti,
                                    strkrprc = order_details_stage.rprc,
                                    discqty = order_details_stage.dscqty,
                                    product = order_details_stage.prd,
                                    statusintr = order_details_stage.st_intrn,
                                    remarks = order_details_stage.remarks,
                                    exchordid = order_details_stage.exchordid,
                                    rqty = order_details_stage.rqty,
                                    mkt_protection = order_details_stage.mkt_protection,
                                    rejreason = order_details_stage.rejreason 
                            FROM 	order_details_stage
                            WHERE 	order_details_stage.norenordno = order_details.orderno;"""

        db_crsr.execute(sql_command)
        db_conn.commit()

        sql_command = """   INSERT OR IGNORE INTO order_details 
                                (orderno,userid,scriptname,quantity,trantype,purchtype,token,orderprice,status,ordertime,
                                exchtime,cancelqty,avgprc,fillshares,trn_status,kidid,actuserid,exchange,ordenttm,retention,
                                multiple,prcftr,dname,priceprc,lotsize,ticksize,strkrprc,discqty,product,statusintr,remarks,
                                exchordid,rqty,mkt_protection,rejreason)
                            SELECT norenordno,uid,tsym,qty,trantype,prctyp,token,prcftr,status,norentm,
                                    exch_tm,cancelqty,avgprc,fillshares, stat,kidid,actid,exch,ordenttm,ret,
                                    mult,prc,dname,pp,ls,ti,rprc,dscqty,prd,st_intrn,remarks,
                                    exchordid,rqty,mkt_protection,rejreason
                            FROM order_details_stage
                            ORDER BY norenordno;"""
        db_crsr.execute(sql_command)
        db_conn.commit()

        sql_command = " DELETE FROM order_details_stage;"
        db_crsr.execute(sql_command)
        db_conn.commit()

        db_crsr.close()
        db_conn.close()

    except Exception as err:
        print(' Exception From ft_update_order')
        print(f"Unexpected {err=}, {type(err)=}")
