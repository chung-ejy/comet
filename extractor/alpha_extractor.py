from datetime import datetime
import requests as r
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()
alpha_token = os.getenv("ALPHA")

class TiingoExtractor(object):

    @classmethod
    def extract(self,ticker,start,end):
        try:
            headers = {
                "Content-Type":"application/json"
            }

            params = {
                "token":token,
                "startDate":start,
                "endDate":end
            }
            url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
            requestResponse = requests.get(url,headers=headers,params=params)
            return pd.DataFrame(requestResponse.json())
        except Exception as e:
            print(str(e))
    
    @classmethod
    def crypto_daily(self,crypto,start,end):
        try:
            headers = {
                "Content-Type":"application/json"
            }
            params = {
                "token":token,
                "startDate":start,
                "endDate":end
            }
            url = f"https://api.tiingo.com/tiingo/crypto/prices?tickers={crypto}usd,fldcbtc&resampleFreq=1day"
            requestResponse = requests.get(url,headers=headers,params=params)
            return requestResponse
        except Exception as e:
            print(str(e))
    
    @classmethod
    def crypto_intraday(self,crypto,start,end):
        try:
            headers = {
                "Content-Type":"application/json"
            }
            params = {
                "token":token,
                "startDate":start,
                "endDate":end
            }
            url = f"https://api.tiingo.com/tiingo/crypto/prices?tickers={crypto}usd,fldcbtc&resampleFreq=5min"
            requestResponse = requests.get(url,headers=headers,params=params)
            return requestResponse
        except Exception as e:
            print(str(e))

