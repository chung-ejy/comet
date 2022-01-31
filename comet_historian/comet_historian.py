import requests as r
import os
from dotenv import load_dotenv
load_dotenv()
key = os.getenv("HISTORIANKEY")
import json
class CometHistorian(object):
    
    @classmethod
    def entry_analysis(self,entry_strategy,merged,signal,value,conservative,req):
        base_url = "https://cometchaserapi.herokuapp.com/api/analysis/"
        headers = {
            "Content-type":"application/json",
            "X-Api-Key":key
        }
        stuff = {"entry_strategy":entry_strategy,"data":merged.to_dict("records")
        ,"signal":signal,"value":value,"conservative":conservative,
        "key":str(key),"side":"entry","req":req}
        response = r.post(base_url,headers=headers,data=json.dumps(stuff).encode("utf-8"))
        return response.json()

    @classmethod
    def exit_analysis(self,exit_strategy,order,merged,req):
        base_url = "https://cometchaserapi.herokuapp.com/api/analysis/"
        headers = {
            "Content-type":"application/json",
            "X-Api-Key":key
        }
        stuff = {"exit_strategy":exit_strategy,"order":order,"data":merged.to_dict("records"),"req":req,"key":key,"side":"exit"}
        response = r.post(base_url,headers=headers,data=json.dumps(stuff).encode("utf-8"))
        return response.json()
    
    @classmethod
    def get_symbols(self):
        base_url = "https://cometchaserapi.herokuapp.com/api/backtest/"
        headers = {
            "Content-type":"application/json",
            "X-Api-Key":key
        }
        response = r.get(base_url,headers=headers)
        return response.json()