from django.http.response import JsonResponse
from django.shortcuts import render
import pickle
import pandas as pd
import json
from django.views.decorators.csrf import csrf_exempt
from datetime import datetime, timedelta
from pymongo import MongoClient
import certifi
from dotenv import load_dotenv
import os
load_dotenv()
token = os.getenv("MONGO_URI")
# Create your views here
## initial loading
client = MongoClient(token,tlsCAFile=certifi.where())
db = client["mainstbets"]
table = db["recent"]
data = table.find(show_record_id=False)
ts = pd.DataFrame(list(data)).drop("_id",axis=1)[["date","ticker","adjClose","rolling","gain"]]

table = db["sp500"]
data = table.find(show_record_id=False)
sector_list = pd.DataFrame(list(data)).drop("_id",axis=1)[["Symbol","Security","GICS Sector"]]
client.close()

ts = ts.merge(sector_list.rename(columns={"Symbol":"ticker"}),on="ticker",how="left")
@csrf_exempt
def timeseries(request):
    complete = {}
    ts["gain"] =[round(x,2) for x in ts["gain"]]
    ts["rolling"] =[round(x,2) for x in ts["rolling"]]
    ts["date"] =[str(x).split("T")[0].split(" ")[0] for x in ts["date"]]
    complete["timeseries"] = list(ts[["GICS Sector","Security","ticker","adjClose","rolling","gain"]].sort_values("gain",ascending=False).to_dict("records"))
    return JsonResponse(complete,safe=False)

@csrf_exempt
def sectors(request):
    complete = {}
    print(sector_list.columns)
    complete["sectors"] = list(sector_list.sort_values("Symbol",ascending=True).to_dict("records"))
    return JsonResponse(complete,safe=False)

@csrf_exempt
def stock(request):
    data = json.loads(request.body.decode("utf-8"))
    try:
        ticker = data["ticker"]
        db = client["mainstbets"]
        table = db["full"]
        data = table.find({"ticker":ticker},show_record_id=False)
        ticker_data = pd.DataFrame(list(data)).drop("_id",axis=1)
        complete = {}
        complete["stock"] = list(ticker_data.to_dict("records"))
    except Exception as e:
        print(str(e))
        complete = {"stock":[]}
    return JsonResponse(complete,safe=False)

@csrf_exempt
def sector(request):
    data = json.loads(request.body.decode("utf-8"))
    try:
        industry = data["sector"]
        industry_data = ts[ts["GICS Sector"]==industry]
        complete = {}
        complete["sector"] = list(industry_data.to_dict("records"))
    except Exception as e:
        print(str(e))
        complete = {"sector":[]}
    return JsonResponse(complete,safe=False)
