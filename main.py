import pandas as pd
from datetime import datetime, timedelta
from time import sleep
from database.comet import Comet
from coinbase.coinbase import Coinbase
from processor.processor import Processor as p
from comet_historian.comet_historian import CometHistorian as comet_hist
from comet_roster.comet_roster import CometRoster as comet_roster
import pytz
status = "initial_load"
live = True
fee = 0.005
key_suffixs = {"live":"","test":"sandbox"}
minimum_trade_ids = {"live":54502987,"test":37559900}
time_to_run = 300
while live:
    for bot_version in ["live","test"]:
        comet = Comet(bot_version)
        comet.cloud_connect()
        roster = pd.DataFrame(comet_roster.get_roster())
        live_users = roster[roster[bot_version]==True]
        key_suffix = key_suffixs[bot_version]
        sleep_time = int(time_to_run / live_users.index.size)
        for user in live_users["username"].unique():
            try:
                trading_params = comet_roster.get_trade_parameters(bot_version,user)
                trading_params = trading_params["trade_params"]
                whitelist_symbols = trading_params["whitelist_symbols"]
                positions =  int(trading_params["positions"])
                retrack_days = int(trading_params["retrack_days"])
                req = float(trading_params["req"])
                signal = float(trading_params["signal"])
                value = trading_params["value"]
                conservative = trading_params["conservative"]
                entry_strategy = trading_params["entry_strategy"]
                exit_strategy = trading_params["exit_strategy"]
                minimum_rows = int(retrack_days * 3)
                end = datetime.now().astimezone(pytz.UTC)
                start = (end - timedelta(days=30)).astimezone(pytz.UTC)
                secrets = comet_roster.get_secrets(user)
                cbs = Coinbase(bot_version,secrets[f"{key_suffix}apikey"],secrets[f"{key_suffix}secret"],secrets[f"{key_suffix}passphrase"])
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
                        error_message = {"date":datetime.now(),"status":status,"message":str(e),"currency":currency}
                        error_message["username"] = user
                        comet.store(f"cloud_{bot_version}_errors",pd.DataFrame([error_message]))
                        continue
                current_spots = pd.DataFrame(spots)
                accounts = accounts.rename(columns={"currency":"crypto"}).merge(current_spots[["crypto","price"]],on="crypto",how="left")
                accounts["available"]  = accounts["available"].astype(float)
                accounts["price"]  = accounts["price"].astype(float)
                accounts["pv"] = accounts["available"] * accounts["price"]
                accounts.rename(columns={"crypto":"currency"},inplace=True)
                pv = sum(accounts["pv"])
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
                        error_message = {"date":datetime.now(),"status":status,"message":str(e),"currency":currency}
                        error_message["username"] = user
                        comet.store(f"cloud_{bot_version}_errors",pd.DataFrame([error_message]))
                        continue
                final = pd.DataFrame(ns)
                merged = final.merge(current_spots.drop("volume",axis=1),on="crypto")
                merged["ask"] = [float(x) for x in merged["ask"]]
                merged["bid"] = [float(x) for x in merged["bid"]]
                merged["price"] = [float(x) for x in merged["price"]]
                merged["username"] = user
                comet.store(f"cloud_{bot_version}_historicals",merged)
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
                    existing_fills = comet.retrieve_fills(user)
                    if existing_fills.index.size > 1:
                        existing_order_ids = list(existing_fills["order_id"])
                    else:
                        existing_order_ids = []
                    new_fills = fills[~fills["order_id"].isin(existing_order_ids) & fills["trade_id"] >  minimum_trade_ids[bot_version]]
                    status = "fills"
                    if new_fills.index.size > 0:
                        new_buys = new_fills[new_fills["side"]=="buy"]
                        new_buys["size"] = [float(x) for x in new_buys["size"]]
                        new_buys["price"] = [float(x) for x in new_buys["price"]]
                        for oi in new_buys["order_id"].unique():
                            order_trades = new_buys[new_buys["order_id"]==oi]
                            if len([x for x in order_trades["settled"] if x == False]) == 0 and order_trades.index.size > 0:
                                order_trades["username"] = user
                                comet.store(f"cloud_{bot_version}_fills",order_trades)
                                comet.store(f"cloud_{bot_version}_completed_buys",order_trades)
                        status = "trade_completes"
                        new_sells = new_fills[(new_fills["side"]=="sell")]
                        new_sells["size"] = [float(x) for x in new_sells["size"]]
                        new_sells["price"] = [float(x) for x in new_sells["price"]]
                        for soi in new_sells["order_id"].unique():
                            sell_order_trades = new_sells[new_sells["order_id"]==soi]
                            if len([x for x in sell_order_trades["settled"] if x == False]) == 0:
                                sell_order_trades["username"] = user
                                comet.store(f"cloud_{bot_version}_fills",sell_order_trades)
                                comet.store(f"cloud_{bot_version}_completed_sells",sell_order_trades)
                status = "sells"
                completed_buys = comet.retrieve(f"cloud_{bot_version}_completed_buys")
                if completed_buys.index.size > 0:
                    completed_buys["price"] = [float(x) for x in completed_buys["price"]]
                    completed_buys["size"] = [float(x) for x in completed_buys["size"]]
                    completed_trades = comet.retrieve(f"cloud_{bot_version}_pending_trades")
                    if completed_trades.index.size > 0:
                        completed_trade_buy_ids = list(completed_trades["order_id"].unique())
                    else:
                        completed_trade_buy_ids = []
                    incomplete_trades = completed_buys[~completed_buys["order_id"].isin(completed_trade_buy_ids)]
                    if incomplete_trades.index.size > 0:
                        incomplete_trades = p.live_column_date_processing(incomplete_trades.rename(columns={"created_at":"date"}))
                        incomplete_trades= incomplete_trades[incomplete_trades["trade_id"]> minimum_trade_ids[bot_version]]
                        for oi in incomplete_trades["order_id"].unique():
                            order = incomplete_trades[incomplete_trades["order_id"]==oi] \
                                            .groupby(["order_id","product_id"]) \
                                            .agg({"date":"first","price":"mean","size":"sum"}).reset_index().iloc[0]
                            ticker_merged = merged[merged["crypto"]==order["product_id"].split("-")[0]]
                            order["date"] = str(order["date"])
                            trade = comet_hist.exit_analysis(exit_strategy,order.to_dict(),ticker_merged,req)
                            if "sell_price" in trade:
                                sell_statement = cbs.place_sell(trade["product_id"]
                                                                            ,trade["sell_price"]
                                                                            ,trade["size"])
                                sell_statement["username"] = user
                                comet.store(f"cloud_{bot_version}_pending_sells",pd.DataFrame([sell_statement]))
                                trade["sell_id"] = sell_statement["id"]
                                trade["username"] = user
                                comet.store(f"cloud_{bot_version}_pending_trades",pd.DataFrame([trade]))
                status = "buys"
                data = cbs.get_orders()
                if balance > float(pv * (positions-fee)) and data.index.size < positions and data.index.size > 0:
                    offerings = comet_hist.entry_analysis(entry_strategy,merged,signal,value,conservative)
                    offerings = pd.DataFrame(offerings)
                    if offerings.index.size > 0:
                        for pos_num in range(min(offerings.index.size,positions)):
                            trade = offerings.iloc[pos_num]
                            buy_price = float(trade["bid"])
                            symbol = trade["crypto"]
                            round_value = 2
                            for i in range(2,9):
                                if float(balance / buy_price) > 10 **-i:
                                    round_value = i + 1
                                    break
                                else:
                                    continue
                            size = round(float(balance/(buy_price*(1+fee))),round_value)
                            buy = cbs.place_buy(symbol,buy_price,size)
                            buy["username"] = user
                            if "message" not in buy.keys():
                                comet.store(f"cloud_{bot_version}_pending_buys",pd.DataFrame([buy]))
                            else:
                                buy["date"] = datetime.now()
                                buy["crypto"] = symbol
                                buy["size"] = size
                                buy["buy_price"] = buy_price
                                buy["balance"] = balance
                                buy["status"] = status
                                comet.store(f"cloud_{bot_version}_errors",pd.DataFrame([buy]))
                status = "recording completed_trades"
                comet.cloud_connect()
                pending_trades = comet.retrieve_pending_trades(user)
                complete_trades = comet.retrieve_completed_trades(user)
                complete_sells = comet.retrieve_completed_sells(user)
                complete_buys = comet.retrieve_completed_buys(user)
                complete_sell_ids = []
                complete_buy_ids = []
                complete_trade_ids = []
                if complete_sells.index.size > 0:
                    complete_sell_ids = complete_sells["order_id"].unique()
                if complete_buys.index.size > 0:
                    complete_buy_ids = complete_buys["order_id"].unique()
                if complete_trades.index.size > 0:
                    complete_trade_ids = complete_trades["order_id"].unique()
                if pending_trades.index.size > 0:
                    complete_trades = pending_trades[(~pending_trades["order_id"].isin(complete_trade_ids)) & (pending_trades["order_id"].isin(complete_buy_ids)) & (pending_trades["sell_id"].isin(complete_sell_ids))]
                    complete_trades["fee"] = [float(x) for x in complete_trades["fee"]]
                    ct = complete_trades.groupby(["product_id","order_id"]).agg({"sell_price":"mean","size":"sum","fee":"sum","price":"mean","date":"first"}).reset_index()
                    if ct.index.size > 0:
                        ct["username"] = user
                        comet.store(f"cloud_{bot_version}_completed_trades",ct)
                status = "iteration_log"
                iteration_data = {"date":datetime.now(),
                                    "retrack_days" : retrack_days
                                    ,"req" : req
                                    ,"signal" : signal
                                    ,"value" : value
                                    ,"conservative" : conservative
                                    ,"entry_strategy" : entry_strategy
                                    ,"exit_strategy" : exit_strategy
                                    ,"positions":positions
                                    ,"fee" : fee
                                    ,"minimum_rows" : minimum_rows
                                    ,"sleep_time" : sleep_time,
                                    "status":status,
                                "username":user}
                comet.store(f"cloud_{bot_version}_iterations",pd.DataFrame([iteration_data]))
                sleep(sleep_time)
            except Exception as e:
                error_log = {"date":datetime.now(),"status":status,"message":str(e)}
                error_log["username"]=user
                print(error_log)
                comet.store(f"cloud_{bot_version}_errors",pd.DataFrame([error_log]))
                sleep(sleep_time)
        comet.disconnect()