import pandas as pd
from datetime import datetime, timedelta
from analyzer.live_entry_strats import LiveEntryStrats as les
from analyzer.live_exit_strats import LiveExitStrats as lxs
from time import sleep
from database.comet import Comet
from coinbase.coinbase_sandbox import CoinbaseSandbox as cbs
from processor.processor import Processor as p
import pytz

comet = Comet()

whitelist_symbols = ['ADA', 'BTC', 'DOGE', 'ETH', 'SHIB', 'WLUNA','AVAX', 'LTC', 'DOT','MATIC']

retrack_days = 3
req = 0.02
signal = 0.01
value = True
conservative = True
entry_strategy = "standard"
exit_strategy = "hold"
fee = 0.005
minimum_rows = retrack_days * 3
live = True
sleep_time = 300
while live:
##CONSTANTS
    comet.cloud_connect()
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
    comet.store("cloud_test_coinbase_hourly",merged)
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
                        comet.store("cloud_test_fills",order_trades)
                        incomplete_trade = lxs.exit_analysis(exit_strategy,merged,order_trades,retrack_days,req)
                        if "sell_price" in incomplete_trade.keys():
                            sell_statement = cbs.place_sell(incomplete_trade["product_id"]
                                                            ,incomplete_trade["sell_price"]
                                                            ,incomplete_trade["size"])
                            comet.store("cloud_test_orders",pd.DataFrame([sell_statement]))
                            incomplete_trade["sell_id"] = sell_statement["id"]
                            comet.store("cloud_test_incomplete_trades",pd.DataFrame([incomplete_trade]))
                completed_trades = new_fills[(new_fills["side"]=="sell")]
                for soi in completed_trades["order_id"].unique():
                    sell_order_trades = completed_trades[completed_trades["order_id"]==soi]
                    if len([x for x in sell_order_trades["settled"] if x == False]) == 0:
                        comet.store("cloud_test_fills",sell_order_trades)
                        complete_trade = sell_order_trades.iloc[0]
                        order_id = complete_trade["order_id"]
                        one_half = comet.retrieve_incomplete_trade(order_id)
                        one_half["sell_date"] = complete_trade["created_at"]
                        one_half["sell_price"] = complete_trade["price"]
                        comet.store("cloud_test_complete_trades",one_half)
    # ##buys
    if balance > 1:
        offerings = les.entry_analysis(entry_strategy,merged,signal,value,conservative)
        if offerings.index.size > 0:
            trade = offerings.iloc[0]
            buy_price = float(trade["bid"])
            symbol = trade["crypto"]
            size = round(float(balance/(buy_price*(1+fee))),6)
            buy = cbs.place_buy(symbol,buy_price,size)
            comet.store("cloud_test_orders",pd.DataFrame([buy]))
    comet.disconnect()
    sleep(sleep_time)