import requests as r
import os
from dotenv import load_dotenv
load_dotenv()
key = os.getenv("ROSTERKEY")
import json
class CometRoster(object):
    
    @classmethod
    def get_roster(self):
        base_url = "https://cometroster.herokuapp.com/api/roster/"
        headers = {
            "Content-type":"application/json",
            "x-api-key":key
        }
        response = r.get(base_url,headers=headers,params={"data_request":"all"})
        return response.json()

    @classmethod
    def get_trade_parameters(self,version,username):
        base_url = "https://cometroster.herokuapp.com/api/trade_params/"
        headers = {
            "Content-type":"application/json",
            "x-api-key":key
        }
        params = {"username":username,"version":version}
        response = r.get(base_url,headers=headers,params=params)
        return response.json()

    @classmethod
    def get_secrets(self,username):
        base_url = "https://cometroster.herokuapp.com/api/treasure/"
        headers = {
            "Content-type":"application/json",
            "x-api-key":key
        }
        params = {"username":username}
        response = r.get(base_url,headers=headers,params=params)
        return response.json()