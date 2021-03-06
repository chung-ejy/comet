import pandas as pd
from processor.processor import Processor as p
class LiveExitStrats(object):
    
    @classmethod
    def exit_analysis(self,exit_strat,incomplete_trade,final,req):
        size = round(incomplete_trade["size"],6)
        buy_price = incomplete_trade["price"]
        product_id = incomplete_trade["product_id"]
        symbol = product_id.split("-")[0]
        incomplete_trade["size"] = size
        incomplete_trade["exit_strat"] = exit_strat
        product_data =  final[(final["crypto"]==symbol)]
        product_data["delta"] = (product_data["price"] - buy_price) / buy_price
        if exit_strat == "due_date":
            analysis = self.due_date(product_data,req)
        else:
            if exit_strat =="hold":
                analysis = self.hold(product_data,req)
            else:
                if exit_strat =="adaptive_due_date":
                    analysis = self.adaptive_due_date(product_data,req)
                else:
                    if exit_strat =="adaptive_hold":
                        analysis = self.adaptive_hold(product_data,req)
                    else:
                        analysis = pd.DataFrame([{}])
        if analysis.index.size > 0:
            incomplete_trade["sell_price"] = product_data["ask"].item()
        return incomplete_trade

    @classmethod
    def due_date(self,final,req):
        profits = final[(final["delta"] >= req)]
        return profits
    
    @classmethod
    def hold(self,final,req):
        profits = final[(final["delta"] >= req)]
        return profits
    
    @classmethod
    def adaptive_hold(self,final,req):
        profits = final[(final["delta"] > req)
                        & (final["p_sign_change"]==True)
                        & (final["velocity"] <= 3)
                        & (final["velocity"] > 0)
                        & (final["inflection"] <= 1)
                        & (final["inflection"] >= -1)]
        return profits
    
    @classmethod
    def adaptive_due_date(self,final,req):
        profits = final[(final["delta"] >= req)
                        & (final["p_sign_change"]==True)
                        & (final["velocity"] <= 3)
                        & (final["velocity"] > 0)
                        & (final["inflection"] <= 1)
                        & (final["inflection"] >= -1)]
        return profits