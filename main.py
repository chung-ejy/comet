import pandas as pd
from datetime import datetime, timedelta
from analyzer.live_entry_strats import LiveEntryStrats as les
from analyzer.live_exit_strats import LiveExitStrats as lxs
from time import sleep
from database.comet import Comet
from coinbase.coinbase import Coinbase as cbs
from processor.processor import Processor as p
import pytz

comet = Comet()

whitelist_symbols = [ 
                    'BTC'
                    , 'ADA'
                    , 'DOGE'
                    , 'ETH'
                    , 'SHIB'
                    , 'WLUNA'
                    ,'AVAX'
                    , 'LTC'
                    , 'DOT'
                    ,'MATIC'
                    ]
live = True
sleep_time = 600
minimum_funds = 50
comet.cloud_connect()
backtest_results = comet.retrieve("trading_params")
trading_params = backtest_results.sort_values("pv",ascending=False).iloc[0]
retrack_days = trading_params["retrack_days"]
req = trading_params["req"]
signal = trading_params["signal"]
value = trading_params["value"]
conservative = trading_params["conservative"]
entry_strategy = trading_params["entry_strategy"]
exit_strategy = trading_params["exit_strategy"]
fee = 0.005
minimum_rows = retrack_days * 3
# print(retrack_days,req,signal,value,conservative,entry_strategy,exit_strategy)
while live:
    try:
        iteration_data = {"date":datetime.now(),
                            "retrack_days" : retrack_days
                            ,"req" : req
                            ,"signal" : signal
                            ,"value" : value
                            ,"conservative" : conservative
                            ,"entry_strategy" : entry_strategy
                            ,"exit_strategy" : exit_strategy
                            ,"fee" : fee
                            ,"minimum_rows" : minimum_rows
                            ,"live" : live
                            ,"sleep_time" : sleep_time}
        comet.store("cloud_iterrations",pd.DataFrame([iteration_data]))
        end = datetime.now().astimezone(pytz.UTC)
        start = (end - timedelta(days=30)).astimezone(pytz.UTC)
        accounts = cbs.get_accounts()
        balance = accounts[accounts["currency"]=="USD"]["balance"].iloc[0]
        pending_orders = cbs.get_orders()
        pending_orders = p.live_column_date_processing(pending_orders.rename(columns={"created_at":"date"}))
        spots = []
        historicals = []
        for currency in whitelist_symbols:
            try:
                spot = cbs.get_current_price(currency)
                historical = cbs.get_timeframe_prices(currency,end,minimum_rows)
                spot["crypto"] = currency
                if "message" not in list(spot.keys()):
                    spots.append(spot)
                historicals.append(historical)
            except:
                print(currency)
                continue
        current_spots = pd.DataFrame(spots)
        current_historicals = pd.concat(historicals)
        current_historicals.sort_values("date",inplace=True)
        ns = []
        for crypto in whitelist_symbols:
            try:
                crypto_sim = current_historicals[current_historicals["crypto"]==crypto].copy()
                crypto_sim.sort_values("date",inplace=True)
                crypto_sim["signal"] = crypto_sim["close"].pct_change(retrack_days)
                crypto_sim["velocity"] = crypto_sim["signal"].pct_change()
                crypto_sim["inflection"] = crypto_sim["velocity"].pct_change()
                crypto_sim["p_sign_change"] = [row[1]["velocity"] * row[1]["inflection"] < 0 for row in crypto_sim.iterrows()]
                ns.append(crypto_sim.iloc[-1])
            except Exception as e:
                print(crypto,str(e))
        final = pd.DataFrame(ns)
        merged = final.merge(current_spots.drop("volume",axis=1),on="crypto")
        merged["ask"] = [float(x) for x in merged["ask"]]
        merged["bid"] = [float(x) for x in merged["bid"]]
        merged["price"] = [float(x) for x in merged["price"]]
        comet.store("cloud_coinbase_hourly",merged)
        fls = []
        for currency in accounts["currency"].unique():
            fill = cbs.get_fill(currency)
            if len(fill) > 0:
                try:
                    f = pd.DataFrame(fill)
                    fls.append(f)
                except:
                    continue
        ## sells
        if len(fls) > 0:
            fills = pd.concat(fls)
            #store_non_existing_executed_buy
            existing_fills = comet.retrieve_fills()
            if existing_fills.index.size > 1:
                new_fills = fills[~fills["order_id"].isin(list(existing_fills["order_id"]))]
                if new_fills.index.size > 1:
                    incomplete_trades = new_fills[(new_fills["side"]=="buy")]
                    incomplete_sells = new_fills[(new_fills["side"]=="sell")]
                    incomplete_trades["size"] = [float(x) for x in incomplete_trades["size"]]
                    incomplete_trades["price"] = [float(x) for x in incomplete_trades["price"]]
                    for oi in incomplete_trades["order_id"].unique():
                        order_trades = incomplete_trades[incomplete_trades["order_id"]==oi]
                        if len([x for x in order_trades["settled"] if x == False]) == 0 and order_trades.index.size > 1:
                            comet.store("cloud_fills",order_trades)
                            incomplete_trade = lxs.exit_analysis(exit_strategy,merged,order_trades,retrack_days,req)
                            if "sell_price" in incomplete_trade.keys():
                                sell_statement = cbs.place_sell(incomplete_trade["product_id"]
                                                                ,incomplete_trade["sell_price"]
                                                                ,incomplete_trade["size"])
                                comet.store("cloud_orders",pd.DataFrame([sell_statement]))
                                incomplete_trade["sell_id"] = sell_statement["id"]
                                comet.store("cloud_incomplete_trades",pd.DataFrame([incomplete_trade]))
                    completed_trades = new_fills[(new_fills["side"]=="sell")]
                    for soi in completed_trades["order_id"].unique():
                        sell_order_trades = completed_trades[completed_trades["order_id"]==soi]
                        if len([x for x in sell_order_trades["settled"] if x == False]) == 0:
                            comet.store("cloud_fills",sell_order_trades)
                            complete_trade = sell_order_trades.iloc[0]
                            order_id = complete_trade["order_id"]
                            one_half = comet.retrieve_incomplete_trade(order_id)
                            one_half["sell_date"] = complete_trade["created_at"]
                            one_half["sell_price"] = complete_trade["price"]
                            comet.store("cloud_complete_trades",one_half)
        # ##buys
        if balance > minimum_funds:
            offerings = les.entry_analysis(entry_strategy,merged,signal,value,conservative)
            if offerings.index.size > 0:
                trade = offerings.iloc[0]
                buy_price = float(trade["price"])
                symbol = trade["crypto"]
                size = round(float(balance/(buy_price*(1+fee))),6)
                buy = cbs.place_buy(symbol,buy_price,size)
                if "message" not in buy.keys():
                    comet.store("cloud_orders",pd.DataFrame([buy]))
                else:
                    buy["date"] = datetime.now()
                    buy["crypto"] = symbol
                    buy["size"] = size
                    buy["buy_price"] = buy_price
                    buy["balance"] = balance
                    comet.store("cloud_errors",pd.DataFrame([buy]))
    except Exception as e:
        error_log = {"date":datetime.now(),"message":str(e)}
        comet.store("cloud_errors",pd.DataFrame([error_log]))
    sleep(sleep_time)
comet.disconnect()