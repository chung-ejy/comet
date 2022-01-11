from django.shortcuts import render
from django.http.response import JsonResponse
import pickle
import pandas as pd
import json
from django.views.decorators.csrf import csrf_exempt
from datetime import datetime, timedelta
import requests
from pymongo import MongoClient
@csrf_exempt
def backendView(request):
    try:
        info = json.loads(request.body.decode("utf-8"))
        if request.method == "GET":
            complete = {}
        elif request.method == "DELETE":
            complete = {}
        elif request.method == "UPDATE":
            complete = {}
        elif request.method == "POST":
            complete = {}
        else:
            complete = {}
    except Exception as e:
        complete = info
        complete["prediction"] = "not found"
        print(str(e))
    return JsonResponse(complete,safe=False)