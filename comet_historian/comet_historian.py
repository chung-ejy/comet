import requests as r
import os
from dotenv import load_dotenv
load_dotenv()
key = os.getenv("HISTORIANKEY")
import json
class CometHistorian(object):
    
    @classmethod
    def entry_analysis(self,entry_strategy,merged,signal,value,conservative):
        base_url = "https://comethistorian.herokuapp.com/api/analysis/"
        headers = {
            "Content-type":"application/json"
        }
        stuff = {"entry_strategy":entry_strategy,"merged":merged.to_dict("records")
        ,"signal":signal,"value":value,"conservative":conservative,
        "key":key,"side":"entry"}
        response = r.post(base_url,headers=headers,data=json.dumps(stuff).encode("utf-8"))
        return response.json()

    @classmethod
    def exit_analysis(self,exit_strategy,order,merged,req):
        base_url = "https://comethistorian.herokuapp.com/api/analysis/"
        headers = {
            "Content-type":"application/json"
        }
        stuff = {"exit_strategy":exit_strategy,"order":order,"merged":merged.to_dict("records"),"req":req,"key":key,"side":"exit"}
        response = r.post(base_url,headers=headers,data=json.dumps(stuff).encode("utf-8"))
        return response.json()