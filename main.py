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
live = False
sleep_time = 3600
minimum_funds = 50
while live:
    status = "initial_load"
    comet.cloud_connect()
    try:
        trading_params = comet.retrieve("cloud_trading_params")
        retrack_days = int(trading_params["retrack_days"].item())
        req = trading_params["req"].item()
        signal = trading_params["signal"].item()
        value = trading_params["value"].item()
        conservative = trading_params["conservative"].item()
        entry_strategy = trading_params["entry_strategy"].item()
        exit_strategy = trading_params["exit_strategy"].item()
        fee = 0.005
        minimum_rows = int(retrack_days * 3)
        end = datetime.now().astimezone(pytz.UTC)
        start = (end - timedelta(days=30)).astimezone(pytz.UTC)
        accounts = cbs.get_accounts()
        balance = accounts[accounts["currency"]=="USD"]["balance"].iloc[0]
        spots = []
        historicals = []
        status = "spots"
        for currency in whitelist_symbols:
            try:
                spot = cbs.get_current_price(currency)
                historical = cbs.get_timeframe_prices(currency,start,end,minimum_rows)
                spot["crypto"] = currency
                if "message" not in list(spot.keys()):
                    spots.append(spot)
                historicals.append(historical)
            except Exception as e:
                error_message = {"date":datetime.now(),"message":str(e),"currency":currency}
                comet.store("cloud_errors",pd.DataFrame([error_message]))
                continue
        current_spots = pd.DataFrame(spots)
        current_historicals = pd.concat(historicals)
        current_historicals.sort_values("date",inplace=True)
        ns = []
        status = "calcs"
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
                error_message = {"date":datetime.now(),"message":str(e),"currency":currency}
                comet.store("cloud_errors",pd.DataFrame([error_message]))
                continue
        final = pd.DataFrame(ns)
        merged = final.merge(current_spots.drop("volume",axis=1),on="crypto")
        merged["ask"] = [float(x) for x in merged["ask"]]
        merged["bid"] = [float(x) for x in merged["bid"]]
        merged["price"] = [float(x) for x in merged["price"]]
        comet.store("cloud_coinbase_hourly",merged)
        fls = []
        status = "fills"
        for currency in accounts["currency"].unique():
            fill = cbs.get_fill(currency)
            if len(fill) > 0:
                try:
                    f = pd.DataFrame(fill)
                    fls.append(f)
                except:
                    continue
        ## documenting_fills
        if len(fls) > 0:
            fills = pd.concat(fls)
            #store_non_existing_executed_buy
            existing_fills = comet.retrieve_fills()
            if existing_fills.index.size > 1:
                existing_order_ids = list(existing_fills["order_id"])
            else:
                existing_order_ids = []
            new_fills = fills[~fills["order_id"].isin(existing_order_ids)]
            status = "sells"
            if new_fills.index.size > 0:
                new_buys = new_fills[new_fills["side"]=="buy"]
                new_buys["size"] = [float(x) for x in new_buys["size"]]
                new_buys["price"] = [float(x) for x in new_buys["price"]]
                for oi in new_buys["order_id"].unique():
                    order_trades = new_buys[new_buys["order_id"]==oi]
                    if len([x for x in order_trades["settled"] if x == False]) == 0 and order_trades.index.size > 0:
                        comet.store("cloud_fills",order_trades)
                        comet.store("cloud_completed_buys",order_trades)
                status = "trade_completes"
                new_sells = new_fills[(new_fills["side"]=="sell")]
                new_sells["size"] = [float(x) for x in new_sells["size"]]
                new_sells["price"] = [float(x) for x in new_sells["price"]]
                for soi in new_sells["order_id"].unique():
                    sell_order_trades = new_sells[new_sells["order_id"]==soi]
                    if len([x for x in sell_order_trades["settled"] if x == False]) == 0:
                        comet.store("cloud_fills",sell_order_trades)
                        comet.store("cloud_completed_sells",sell_order_trades)
        status = "sells"
        completed_buys = comet.retrieve("cloud_completed_buys")
        completed_buys["price"] = [float(x) for x in completed_buys["price"]]
        completed_buys["size"] = [float(x) for x in completed_buys["size"]]
        completed_trades = comet.retrieve("cloud_pending_trades")
        if completed_trades.index.size > 0:
            completed_trade_buy_ids = list(completed_trades["order_id"].unique())
        else:
            completed_trade_buy_ids = []
        incomplete_trades = completed_buys[~completed_buys["order_id"].isin(completed_trade_buy_ids)]
        incomplete_trades = p.live_column_date_processing(incomplete_trades.rename(columns={"created_at":"date"}))
        for row in incomplete_trades.iterrows():
            order = row[1]
            trade = lxs.exit_analysis(exit_strategy,order,merged,req)
            if "sell_price" in trade:
                sell_statement = cbs.place_sell(trade["product_id"]
                                                            ,trade["sell_price"]
                                                            ,trade["size"])
                comet.store("cloud_pending_sells",pd.DataFrame([sell_statement]))
                trade["sell_id"] = sell_statement["id"]
                comet.store("cloud_pending_trades",pd.DataFrame([trade]))
        status = "buys"
        data = cbs.get_orders()
        if balance > 900 and data.index.size < 1:
            offerings = les.entry_analysis(entry_strategy,merged,signal,value,conservative)
            if offerings.index.size > 0:
                trade = offerings.iloc[0]
                buy_price = float(trade["bid"])
                symbol = trade["crypto"]
                size = round(float(100/(buy_price*(1+fee))),6)
                buy = cbs.place_buy(symbol,buy_price,size)
                if "message" not in buy.keys():
                    comet.store("cloud_pending_buys",pd.DataFrame([buy]))
                else:
                    buy["date"] = datetime.now()
                    buy["crypto"] = symbol
                    buy["size"] = size
                    buy["buy_price"] = buy_price
                    buy["balance"] = balance
                    comet.store("cloud_errors",pd.DataFrame([buy]))
        status = "iteration_log"
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
                            ,"sleep_time" : sleep_time,
                            "status":status}
        comet.store("cloud_iterrations",pd.DataFrame([iteration_data]))
        sleep(sleep_time)
    except Exception as e:
        error_log = {"date":datetime.now(),"message":str(e)}
        comet.store("cloud_errors",pd.DataFrame([error_log]))
    comet.disconnect()