from datetime import timedelta, datetime
import pandas as pd

class ExitStrats(object):
    
    @classmethod
    def due_date(self,final,trade,rt,req):
        symbol = trade["crypto"]
        exits = final[(final["date"]>trade["date"]) & (final["crypto"]==symbol)]
        bp = trade["value"]
        due_date = trade["date"]+timedelta(days=rt)
        exits["delta"] = (exits["value"] - bp) / bp
        profits = exits[(exits["delta"] >= req) & (exits["date"] <= due_date)]
        breakeven = exits[(exits["delta"]>=0) & (exits["date"] > due_date)]
        if profits.index.size < 1:
            if breakeven.index.size < 1:
                the_exit = exits[(exits["date"] > due_date)].iloc[-1]
                trade["sell_price"] = the_exit["value"]
                trade["type"] = "loss"
            else:
                the_exit = breakeven.iloc[0]
                trade["type"] = "breakeven"
                trade["sell_price"] = bp
        else:
            the_exit = profits.iloc[0]
            trade["type"] = "profit"
            trade["sell_price"] = bp * (1+req)
        trade["sell_date"] = the_exit["date"]
        trade["buy_price"] = bp
        trade["delta"] = (trade["sell_price"] - trade["buy_price"])/ trade["buy_price"]
        return trade
    
    @classmethod
    def hold(self,final,trade,rt,req):
        symbol = trade["crypto"]
        exits = final[(final["date"]>trade["date"]) & (final["crypto"]==symbol)]
        bp = trade["value"]
        exits["delta"] = (exits["value"] - bp) / bp
        profits = exits[(exits["delta"] >= req) & (exits["date"] > trade["date"])]
        if profits.index.size < 1:
            the_exit = exits.iloc[-1]
            trade["type"] = "held"
            trade["sell_price"] = the_exit["value"]
        else:
            the_exit = profits.iloc[0]
            trade["type"] = "profit"
            trade["sell_price"] = bp * (1+req)
        trade["sell_date"] = the_exit["date"]
        trade["buy_price"] = bp
        trade["delta"] = (trade["sell_price"] - trade["buy_price"])/ trade["buy_price"]
        return trade
    
    @classmethod
    def adaptive_hold(self,final,trade,rt,req):
        symbol = trade["crypto"]
        exits = final[(final["date"]>trade["date"]) & (final["crypto"]==symbol)]
        bp = trade["value"]
        exits["delta"] = (exits["value"] - bp) / bp
        profits = exits[(exits["date"] > trade["date"])
                        & (exits["delta"] > req)
                        & (exits["p_sign_change"]==True)
                        & (exits["velocity"] <= 3)
                        & (exits["velocity"] > 0)
                        & (exits["inflection"] <= 1)
                        & (exits["inflection"] >= -1)]
        if profits.index.size < 1:
            the_exit = exits.iloc[-1]
            trade["type"] = "held"
        else:
            the_exit = profits.iloc[0]
            trade["type"] = "profit"
        trade["sell_price"] = the_exit["value"]
        trade["sell_date"] = the_exit["date"]
        trade["buy_price"] = bp
        trade["delta"] = (trade["sell_price"] - trade["buy_price"])/ trade["buy_price"]
        return trade
    
    @classmethod
    def adaptive_due_date(self,final,trade,rt,req):
        symbol = trade["crypto"]
        exits = final[(final["date"]>trade["date"]) & (final["crypto"]==symbol)]
        bp = trade["value"]
        exits["delta"] = (exits["value"] - bp) / bp
        due_date = trade["date"]+timedelta(days=rt)
        profits = exits[(exits["date"] <= due_date)
                        & (exits["delta"] >= req)
                        & (exits["p_sign_change"]==True)
                        & (exits["velocity"] <= 3)
                        & (exits["velocity"] > 0)
                        & (exits["inflection"] <= 1)
                        & (exits["inflection"] >= -1)].sort_values("date")
        breakeven = exits[(exits["delta"]>=0) & (exits["date"] > due_date)].sort_values("date")
        if profits.index.size < 1:
            if breakeven.index.size < 1:
                the_exit = exits[(exits["date"] > due_date)].iloc[-1]
                trade["sell_price"] = the_exit["value"]
                trade["type"] = "loss"
            else:
                the_exit = breakeven.iloc[0]
                trade["type"] = "breakeven"
                trade["sell_price"] = bp
        else:
            the_exit = profits.iloc[0]
            trade["type"] = "profit"
            trade["sell_price"] = the_exit["value"]
        trade["sell_date"] = the_exit["date"]
        trade["buy_price"] = bp
        trade["delta"] = (trade["sell_price"] - trade["buy_price"]) / trade["buy_price"]
        return trade