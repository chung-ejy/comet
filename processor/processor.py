import pandas as pd
import pytz
class Processor(object):
    
    @classmethod
    def column_date_processing(self,data):
        new_cols = {}
        for column in data.columns:
            new_cols[column] = column.lower().replace(" ","")
        for col in new_cols:
            data.rename(columns={col:new_cols[col]},inplace=True)
        if "date" in list(data.columns):
            try:
                data["date"] = pd.to_datetime(data["Date"]).dt.tz_localize(None)
            except:
                data["date"] = pd.to_datetime(data["date"]).dt.tz_localize(None)
        return data
    
    @classmethod
    def live_column_date_processing(self,data):
        new_cols = {}
        for column in data.columns:
            new_cols[column] = column.lower().replace(" ","")
        for col in new_cols:
            data.rename(columns={col:new_cols[col]},inplace=True)
        if "date" in list(data.columns):
            try:
                data["date"] = [x.astimezone(pytz.UTC) for x in pd.to_datetime(data["date"])]
            except:
                data["date"] = [x.astimezone(pytz.UTC) for x in pd.to_datetime(data["date"])]
        return data