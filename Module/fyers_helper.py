from fyers_api import accessToken, fyersModel
from urllib.parse import parse_qs,urlparse
import sys
import requests
import pyotp
import time
import json

# API endpoints

BASE_URL = "https://api-t2.fyers.in/vagator/v2"
BASE_URL_2 = "https://api.fyers.in/api/v2"
PIN_VERIFY_URL = "https://api-t2.fyers.in/vagator/v2/verify_pin"
TOKEN_URL = "https://api.fyers.in/api/v2/token"
URL_SEND_LOGIN_OTP = BASE_URL + "/send_login_otp"   #/send_login_otp_v2
URL_VERIFY_TOTP = BASE_URL + "/verify_otp"
URL_VERIFY_PIN = BASE_URL + "/verify_pin"
URL_TOKEN = BASE_URL_2 + "/token"
URL_VALIDATE_AUTH_CODE = BASE_URL_2 + "/validate-authcode"
SUCCESS = 1
ERROR = -1

def send_login_otp(fy_id, app_id):
    try:
        result_string = requests.post(url=URL_SEND_LOGIN_OTP, json= {"fy_id": fy_id, "app_id": app_id })
        if result_string.status_code != 200:
            return [ERROR, result_string.text]
        result = json.loads(result_string.text)
        request_key = result["request_key"]
        return [SUCCESS, request_key]
    except Exception as e:
        return [ERROR, e]
    
def verify_totp(request_key, totp):
    try:
        result_string = requests.post(url=URL_VERIFY_TOTP, json={"request_key": request_key,"otp": totp})
        if result_string.status_code != 200:
            return [ERROR, result_string.text]
        result = json.loads(result_string.text)
        request_key = result["request_key"]
        return [SUCCESS, request_key]
    except Exception as e:
        return [ERROR, e]

def fyers_get_session(config):

    FILE_PATH = f"{config['token_file']}"
    APP_ID =  config['apikey'] 
    SECRET_KEY = config['apisecret']
    APP_TYPE = config['apptype']
    client_id= f'{APP_ID}-{APP_TYPE}'

    FY_ID = config['username']  # Your fyers ID
    APP_ID_TYPE = "2"  # Keep default as 2, It denotes web login
    TOTP_KEY = config['totpkey']  # TOTP secret is generated when we enable 2Factor TOTP from myaccount portal
    PIN = config['user_pin']  # User pin for fyers account
    REDIRECT_URI = config['loginurl']
    LOG_PATH = config['log_path']

    try:

        auth_file_object = open(f"{FILE_PATH}", "r+")
        access_token = auth_file_object.readline()

        fyers = fyersModel.FyersModel(
            client_id=client_id, token=access_token, log_path=LOG_PATH)
        is_async = True
        # Set to true in you need async API calls
        auth_file_object.close()

        validate = fyers.get_profile()

        if "error" in validate["s"] or "error" in validate["message"] or "expired" in validate["message"]:
            print("Getting a new fyers access token!")
            raise

        return fyers

    except Exception as err:
        print(' Error Connecting to Fyers Api')
        print(f"Unexpected {err=}, {type(err)=}")
        return fyers_login(client_id,APP_ID,APP_TYPE,SECRET_KEY,REDIRECT_URI,FY_ID,APP_ID_TYPE,TOTP_KEY,PIN,LOG_PATH,FILE_PATH)

def fyers_login(client_id,APP_ID,APP_TYPE,SECRET_KEY,REDIRECT_URI,FY_ID,APP_ID_TYPE,TOTP_KEY,PIN,LOG_PATH,FILE_PATH):
    try:
        session = accessToken.SessionModel(client_id=client_id, secret_key=SECRET_KEY, redirect_uri=REDIRECT_URI,
                                    response_type='code', grant_type='authorization_code')

        urlToActivate = session.generate_authcode()
        # print(f'URL to activate APP:  {urlToActivate}')


        # Step 1 - Retrieve request_key from send_login_otp API

        send_otp_result = send_login_otp(fy_id=FY_ID, app_id=APP_ID_TYPE)

        if send_otp_result[0] != SUCCESS:
            print(f"send_login_otp failure - {send_otp_result[1]}")
            sys.exit()
        # else:
        #     print("send_login_otp success")

        # Step 2 - Verify totp and get request key from verify_otp API
        for i in range(1,3):
            request_key = send_otp_result[1]
            verify_totp_result = verify_totp(request_key=request_key, totp=pyotp.TOTP(TOTP_KEY).now())
            if verify_totp_result[0] != SUCCESS:
                # print(f"verify_totp_result failure - {verify_totp_result[1]}")
                time.sleep(1)
            else:
                # print(f"verify_totp_result success {verify_totp_result}")
                break

        request_key_2 = verify_totp_result[1]

        # Step 3 - Verify pin and send back access token
        ses = requests.Session()
        payload_pin = {"request_key":f"{request_key_2}","identity_type":"pin","identifier":f"{PIN}","recaptcha_token":""}
        res_pin = ses.post(PIN_VERIFY_URL, json=payload_pin).json()
        # print(res_pin['data'])
        ses.headers.update({
            'authorization': f"Bearer {res_pin['data']['access_token']}"
        })

        authParam = {"fyers_id":FY_ID,"app_id":APP_ID,"redirect_uri":REDIRECT_URI,"appType":APP_TYPE,"code_challenge":"","state":"None","scope":"","nonce":"","response_type":"code","create_cookie":True}
        authres = ses.post(TOKEN_URL, json=authParam).json()
        # print("This is the URL:",authres)
        url = authres['Url']
        # print("This is the URL:", url)
        parsed = urlparse(url)
        auth_code = parse_qs(parsed.query)['auth_code'][0]

        session.set_token(auth_code)
        response = session.generate_token()
        access_token= response["access_token"]
        # print(access_token)

        auth_file_object = open(f"{FILE_PATH}", "w+")
        auth_file_object.write(access_token)
        auth_file_object.close()        

        fyers = fyersModel.FyersModel(client_id=client_id, token=access_token, log_path=LOG_PATH)
        # print(fyers.get_profile())

        return fyers
    
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        raise

