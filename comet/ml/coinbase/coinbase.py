
import pandas as pd
import requests as r
class Coinbase(object):
    
    @classmethod
    def get_price(self,product_id):
        url = "https://api.exchange.coinbase.com/products/product_id/ticker"
        headers = {"Accept": "application/json"}
        params = {"product_id":product_id}
        response = r.get(url,headers=headers,params=params)
        return response.json()
    
    @classmethod
    def get_account(self):
        url = "https://api.exchange.coinbase.com/accounts"
        headers = {
            "Accept": "application/json",
        }
        response = r.get(url,headers=headers,params=params)
        return response.json()
    
    @classmethod
    def get_orders(self):
        url = "https://api.exchange.coinbase.com/orders?sortedBy=created_at&sorting=desc&limit=100&status=%5B%27open%27%2C%20%27pending%27%5D"

        headers = {
            "Accept": "application/json",
            "cb-access-key": "s51rO9jFeYT4PX2f"
        }

        response = r.request("GET", url, headers=headers)

        return response.json()
        
    @classmethod
    def place_buy(self,product_id,):
        url = "https://api.exchange.coinbase.com/orders"
        payload = {
            "profile_id": "default profile_id",
            "type": "limit",
            "side": "buy",
            "stp": "dc",
            "stop": "loss",
            "time_in_force": "GTC",
            "cancel_after": "min",
            "post_only": "false"
        }
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        response = r.post(url,headers,json=payload)
        return response

    @classmethod
    def place_sell(self):
        url = "https://api.exchange.coinbase.com/orders"
        payload = {
            "profile_id": "default profile_id",
            "type": "limit",
            "side": "sell",
            "stp": "dc",
            "stop": "entry",
            "time_in_force": "GTC",
            "cancel_after": "day",
            "post_only": "false"
        }
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "cb-access-key": "s51rO9jFeYT4PX2f"
        }
        response = r.post(url,headers,json=payload)
        return response