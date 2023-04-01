from math import floor
from binance.client import Client
import pandas as pd
import numpy as np
import datetime
from Management_function import Management
import os
import streamlit as st
from collections import OrderedDict


tickers = ['BTCUSDT','ETHUSDT','BNBUSDT','XRPUSDT', 'ADAUSDT', 'MATICUSDT', 'DOTUSDT', 'AVAXUSDT', 'UNIUSDT', 'LTCUSDT', 'ATOMUSDT',
            'LINKUSDT', 'ETCUSDT', 'ALGOUSDT', 'XMRUSDT', 'NEARUSDT', 'BCHUSDT', 'FILUSDT', 'APEUSDT', 'EGLDUSDT', 'SANDUSDT', 'AAVEUSDT', 'AXSUSDT',
            'EOSUSDT','CRVUSDT','MANAUSDT','ENSUSDT','GMTUSDT','FLOWUSDT','ARUSDT','XLMUSDT','KLAYUSDT','APTUSDT','FTMUSDT','DYDXUSDT',
            'WAVESUSDT','GALAUSDT','OPUSDT','SNXUSDT','SUSHIUSDT', 'VETUSDT', 'KNCUSDT']

# tickers = ['APEUSDT']

# account_number = int(input('Input your account number: '))


# client, tickers = Management.account(account_number)
# start_time= datetime.datetime(2023, 2, 25, 0, 0, 0)
# end_time= datetime.datetime(2023, 3, 4, 0, 0, 0)

# start_time_ms = int(start_time.timestamp() *1000)
# end_time_ms = int(end_time.timestamp() * 1000)
order_history = pd.read_excel('historical_trades.xlsx')
df_oh = pd.DataFrame(order_history).iloc[::-1]
df_oh = df_oh.reset_index(drop=True)
df_tp = df_oh[(df_oh['origType'] == 'TAKE_PROFIT') & (df_oh['status'] == 'FILLED') | (df_oh['origType'] == 'TAKE_PROFIT_MARKET') & (df_oh['status'] == 'FILLED')]


not_corrected_tickers = pd.DataFrame()
tickers_not_double = pd.DataFrame()
non_indexes = []
for tick in tickers:
    # order_history = client.futures_get_all_orders(symbol=tick, startTime=start_time_ms, endTime=end_time_ms, limit=1000)
    sym_orders = df_oh[df_oh['symbol'] == tick]
    df_stops = df_oh[(df_oh['origType'] == 'STOP_MARKET') & (df_oh['status'] == 'FILLED') ]
    # st.write(sym_orders)

    # ################################ ~~~~ ALL OF STOP MARKET SEARCH IN TAKE PROFIT FIELDS ~~~~~ ##################################################

    sym_tp_index = np.array(df_tp[df_tp['symbol'] == tick].index)
    # st.write(sym_orders)
    # st.write(sym_tp_index)
    if len(sym_tp_index) == 0:
        continue

    tp = sym_tp_index[0]
    

    # 1st index of dataframe until take profit filled block 1
    all_tp_head =  sym_orders.loc[:tp-1] 
    # st.write(all_tp_head)
    sm_all_head = np.array(all_tp_head[(all_tp_head['origType'] == 'STOP_MARKET') & (all_tp_head['status'] == 'FILLED')].index)
    # st.write(sm_all_head)
    for i in sm_all_head:
        lim_next = all_tp_head.loc[:i-1]
        
        x = lim_next[(lim_next['origType'] == 'LIMIT') & (lim_next['status'] == 'FILLED') | (lim_next['origType'] == 'MARKET') & (lim_next['status'] == 'FILLED') ].tail(1)

        
        sm_amount = round(float(df_oh.loc[i]['executedQty'])*2.23)
        for g in x['executedQty']:
            lim_amount = round(float(g))
            if int(lim_amount)  != 0:
                if (abs(int(lim_amount) - int(sm_amount))/int(lim_amount))*100 > 2:
                    time = float(df_oh.loc[i]['time']/1000)
                    formatted_time = datetime.datetime.fromtimestamp(time).strftime('%m%d%Y %H:%M:%S')
                    price = float(df_oh.loc[i]['avgPrice'])
                    amount = float(df_oh.loc[i]['executedQty'])
                    not_double = pd.DataFrame({'Symbol' :[tick], 'time':[formatted_time], 'amount':[amount], 'price':[price]})
                    non_indexes.append(i)
                    tickers_not_double = tickers_not_double.append(not_double, ignore_index = True)
                else:
                    pass
        
    tp_next = [] #Stores sym_tp_index[1:last element]
    diffs = [] 
    differences = []       
    if len(sym_tp_index) < 2:
        pass
    else:
        for jj in range(len(sym_tp_index)-1):
            diff = sym_tp_index[jj+1] -sym_tp_index[jj]
            differences.append(diff)

        if len(differences) == 1:    
            for i in range(0, len(differences)):    
                diffs.append(differences[i])
        else:
            for i in range(1, len(differences)):    
                diffs.append(differences[i])

        for x in  range(1, len(sym_tp_index)):
            tp_next.append(sym_tp_index[x])

        for xx in range(0, len(tp_next)):
            if len(sym_tp_index) < 2:
                pass
            # block 2 1st take profit filled until 2nd take profit filled if sym orders have two take profits only
            elif len(sym_tp_index) == 2:
                n = sym_tp_index[0] + 1
                last_tp_next = sym_orders.index
                for a in tp_next:
                    tp_next_head = df_oh.loc[n:a]
                    sm_head01 = np.array(tp_next_head[(tp_next_head['origType'] == 'STOP_MARKET') & (tp_next_head['status'] == 'FILLED')].index)

                    # st.write(tp_next_head)
                    # st.write(tp_next_head)
                    
                    for zz in sm_head01:
                        lim_next01 = tp_next_head.loc[:zz-1]
                        xx = lim_next01[(lim_next01['origType'] == 'LIMIT') & (lim_next01['status'] == 'FILLED') | (lim_next01['origType'] == 'MARKET') & (lim_next01['status'] == 'FILLED') ].tail(1)
                        sm_next_amount = round(float(df_oh.loc[zz]['executedQty'])*2.23)
                        lim_amount = round(float(xx['executedQty']))

                        if int(lim_amount) !=  0:
                                
                            if (abs(int(lim_amount) - floor(sm_next_amount)))/int(lim_amount)*100 > 10:
                                time = float(df_oh.loc[zz]['time']/1000)
                                formatted_time = datetime.datetime.fromtimestamp(time).strftime('%m%d%Y %H:%M:%S')
                                price = float(df_oh.loc[zz]['avgPrice'])
                                amount = float(df_oh.loc[zz]['executedQty'])
                                non_indexes.append(zz)
                                not_double_next = pd.DataFrame({'Symbol' :[tick], 'time':[formatted_time], 'amount':[amount], 'price':[price]})
                                tickers_not_double = tickers_not_double.append(not_double_next, ignore_index = True)
                            else:
                                pass
                            
                # blk 3 2nd take profit filled until last of gathered dataframes if sym orders have two take profits only
                tp_last_head = df_oh.loc[tp_next[-1]:last_tp_next[-1]]
                # st.write(tp_last_head)
                sm_head_02 = np.array(tp_last_head[(tp_last_head['origType'] == 'STOP_MARKET') & (tp_last_head['status'] == 'FILLED')].index)
                # st.write(sm_head_02)
                for vv in sm_head_02:
                    lim_next_02 = tp_last_head.loc[:vv-1]
                    c = lim_next_02[(lim_next_02['origType'] == 'LIMIT') & (lim_next_02['status'] == 'FILLED') | (lim_next_02['origType'] == 'MARKET') & (lim_next_02['status'] == 'FILLED') ].tail(1)
                    # st.write(c)
                    sm_amount_02 = round(float(df_oh.loc[vv]['executedQty'])*2.23)
                    lim_amount_02 = round(float(c['executedQty']))
                    # st.write(sm_amount_02)
                    # st.write(lim_amount_02)
                    if int(lim_amount_02)  != 0:
                            
                        if (abs(int(lim_amount_02) - floor(sm_amount_02)))/int(lim_amount_02)*100 > 10:
                            time = float(df_oh.loc[vv]['time']/1000)
                            formatted_time = datetime.datetime.fromtimestamp(time).strftime('%m%d%Y %H:%M:%S')
                            price = float(df_oh.loc[vv]['avgPrice'])
                            amount = float(df_oh.loc[vv]['executedQty'])
                            non_indexes.append(vv)
                            not_double_last = pd.DataFrame({'Symbol' :[tick], 'time':[formatted_time], 'amount':[amount], 'price':[price]})
                            tickers_not_double = tickers_not_double.append(not_double_last, ignore_index = True)
                        else:
                            pass
                        
                    
        if len(sym_tp_index) <= 2:
            pass
        else:
        #blk 4 1st take profit filled until 2nd take profit filled if sym orders have two o more take profits filleds
            tp_next01 = sym_tp_index[1] #one element
            tp_next.pop(0)
            last_tp_next = sym_orders.index
        
            tp_next_head = df_oh.loc[tp+1:tp_next01]
            sm_head01 = np.array(tp_next_head[(tp_next_head['origType'] == 'STOP_MARKET') & (tp_next_head['status'] == 'FILLED')].index)
            # st.write(tp_next_head)
            # st.write(sm_head01)
            for uu in sm_head01:
                lim_next01 = tp_next_head.loc[:uu-1]
                # st.write(lim_next01)
                
                a = lim_next01[(lim_next01['origType'] == 'LIMIT') & (lim_next01['status'] == 'FILLED') | (lim_next01['origType'] == 'MARKET') & (lim_next01['status'] == 'FILLED') ].tail(1)
                sm_amount01 = round(float(df_oh.loc[uu]['executedQty'])*2.23)
                lim_amount01 = round(float(a['executedQty']))
                

                if int(lim_amount01)  != 0:
                        
                    if (abs(int(lim_amount01) - floor(sm_amount01)))/int(lim_amount01)*100 > 10:
                        time = float(df_oh.loc[uu]['time']/1000)
                        formatted_time = datetime.datetime.fromtimestamp(time).strftime('%m%d%Y %H:%M:%S')
                        price = float(df_oh.loc[uu]['avgPrice'])
                        amount = float(df_oh.loc[uu]['executedQty'])
                        not_double_next = pd.DataFrame({'Symbol' :[tick], 'time':[formatted_time], 'amount':[amount], 'price':[price]})
                        non_indexes.append(uu)
                        tickers_not_double = tickers_not_double.append(not_double_next, ignore_index = True)
                    else:
                        pass
                                
            # blk 5 inner search - 2nd take profit filled until next take profit filled if sym orders have two o more take profits filleds
            # st.write(diffs)
            for aa in range(min(len(tp_next), len(diffs))):
                tp_i = (tp_next[aa] - diffs[aa]) +1 
                tp_head = df_oh.loc[tp_i:tp_next[aa]]
                sm_head02 = np.array(tp_head[(tp_head['origType'] == 'STOP_MARKET') & (tp_head['status'] == 'FILLED')].index)
                # st.write(tp_head)
                # st.write(sm_head02)

                for zz in sm_head02:
                    lim_next02 = tp_head.loc[:zz-1]
                    # st.write(lim_next02)
                    

                    sm_next_amount = round(float(df_oh.loc[zz]['executedQty'])*2.23)
                    
                    xx = lim_next02[(lim_next02['origType'] == 'LIMIT') & (lim_next02['status'] == 'FILLED') | (lim_next02['origType'] == 'MARKET') & (lim_next02['status'] == 'FILLED') ].tail(1)
                    lim_amount = round(float(xx['executedQty']))

                    if int(lim_amount)  != 0:
                        if (abs(int(lim_amount)- floor(sm_next_amount))/int(lim_amount))*100 > 10:
                                time = float(df_oh.loc[zz]['time']/1000)
                                formatted_time = datetime.datetime.fromtimestamp(time).strftime('%m%d%Y %H:%M:%S')
                                price = float(df_oh.loc[zz]['avgPrice'])
                                amount = float(df_oh.loc[zz]['executedQty'])
                                not_double_next = pd.DataFrame({'Symbol' :[tick], 'time':[formatted_time], 'amount':[amount], 'price':[price]})
                                non_indexes.append(zz)
                                tickers_not_double = tickers_not_double.append(not_double_next, ignore_index = True)
                        else:
                            pass
                        
            # blk 6 last take profit filled until last index of dataframe if sym orders have two o more take profits filleds
            tp_last_head = df_oh.loc[tp_next[-1]:last_tp_next[-1]]
            # st.write(tp_last_head)

            sm_head_02 = np.array(tp_last_head[(tp_last_head['origType'] == 'STOP_MARKET') & (tp_last_head['status'] == 'FILLED')].index)
            # st.write(sm_head_02)
        
            for vv in sm_head_02:
                lim_next_02 = tp_last_head.loc[:vv-1]
                # st.write(vv)
                
                c = lim_next_02[(lim_next_02['origType'] == 'LIMIT') & (lim_next_02['status'] == 'FILLED') | (lim_next_02['origType'] == 'MARKET') & (lim_next_02['status'] == 'FILLED') ].tail(1)

                sm_amount_02 = round(float(df_oh.loc[vv]['executedQty'])*2.23)
                lim_amount_02 = round(float(c['executedQty']))
                # st.write(sm_amount_02)
                # st.write(lim_amount_02)

                if int(lim_amount_02)  != 0:
                    
                    if (abs(int(lim_amount_02) - floor(sm_amount_02)))/int(lim_amount_02)*100 > 10:
                        time = float(df_oh.loc[vv]['time']/1000)
                        formatted_time = datetime.datetime.fromtimestamp(time).strftime('%m%d%Y %H:%M:%S')
                        price = float(df_oh.loc[vv]['avgPrice'])
                        amount = float(df_oh.loc[vv]['executedQty'])
                        non_indexes.append(vv)
                        not_double_last = pd.DataFrame({'Symbol' :[tick], 'time':[formatted_time], 'amount':[amount], 'price':[price]})
                        tickers_not_double = tickers_not_double.append(not_double_last, ignore_index = True)
                    else:
                        pass
    # ################################ ~~~~ END OF ALL OF STOP MARKET SEARCH IN TAKE PROFIT FIELDS ~~~~~ ###########################################


# tickers_not_double.to_csv('non_doubling.csv', mode='a', header=not os.path.exists('non_doubling.csv'),index=False)    

# st.write(non_indexes)

# ################################ ~~~~ ALL OF MARKET SEARCH IN STOP MARKET FIELDS ~~~~~ #######################################################



for tick in tickers:
    sym_check = df_oh[df_oh['symbol'] == tick]
    # st.write(sym_check)

    ini_list = []
    for i in non_indexes:
        if i in np.array(sym_check.index):
            ini_list.append(i)

    if ini_list:
        ranges = [(ini_list[0], ini_list[0])]
        for i in ini_list[1:]:
            if i == ranges[-1][1] + 1:
                ranges[-1] = (ranges[-1][0], i)
            else:
                ranges.append((i, i))
        
        flat_ranges = list(OrderedDict.fromkeys(value for range_tuple in ranges for value in range_tuple))
        fr = flat_ranges[0]
        # blk 1 initial sm to initial data of df
        all_sm_head = sym_check.loc[:fr-1] 
        # st.write(all_sm_head)
        limit = float(df_oh.loc[all_sm_head.tail(1).index]['executedQty'])
        market_head = np.array(all_sm_head[(all_sm_head['origType'] == 'MARKET') & (all_sm_head['status'] == 'FILLED')].tail(1).index)

        market_added = round(float(df_oh.loc[market_head]['executedQty']) + limit)
        doubled = round(float(df_oh.loc[fr]['executedQty']) *2.23)
        # st.write(market_added)
        # st.write(doubled)
        if market_added != doubled:
            time = float(df_oh.loc[fr]['time']/1000)
            formatted_time = datetime.datetime.fromtimestamp(time).strftime('%m%d%Y %H:%M:%S')
            price = float(df_oh.loc[fr]['avgPrice'])
            amount = float(df_oh.loc[fr]['executedQty'])
            corrected_not_double = pd.DataFrame({'Symbol' :[tick], 'time':[formatted_time], 'amount':[amount], 'price':[price]})
            not_corrected_tickers = not_corrected_tickers.append(corrected_not_double, ignore_index = True)
        else:
            pass
        
        #not yet tested for flat ranges that have two only element
        

        if len(flat_ranges) < 2:
            pass
        # block 2 1st take profit filled until 2nd take profit filled if sym orders have two take profits only
        elif len(flat_ranges) == 2:
            for ff in range(1, len(flat_ranges)):
            #Assuming that ff is reading the second element only
                n = fr + 1
                last_sm_next = sym_check.index
                sm_next_head = df_oh.loc[n:ff-1]
                market_head01 = np.array(sm_next_head[(sm_next_head['origType'] == 'MARKET') & (sm_next_head['status'] == 'FILLED')].tail(1).index)
                limit = float(df_oh.loc[sm_next_head.tail(1).index]['executedQty'])
                market_added = round(float(df_oh.loc[market_head01]['executedQty']) + limit)
                doubled = round(float(df_oh.loc[flat_ranges[ff]]['executedQty']) *2.23)
                if market_added != doubled:
                    time = float(df_oh.loc[flat_ranges[ff]]['time']/1000)
                    formatted_time = datetime.datetime.fromtimestamp(time).strftime('%m%d%Y %H:%M:%S')
                    price = float(df_oh.loc[flat_ranges[ff]]['avgPrice'])
                    amount = float(df_oh.loc[flat_ranges[ff]]['executedQty'])
                    corrected_not_double = pd.DataFrame({'Symbol' :[tick], 'time':[formatted_time], 'amount':[amount], 'price':[price]})
                    not_corrected_tickers = not_corrected_tickers.append(corrected_not_double, ignore_index = True)
                    # st.write(corrected_not_double)
                else:
                    pass
                sm_last_head = df_oh.loc[ff:last_tp_next[-1]]
                market_head02 = np.array(sm_last_head[(sm_last_head['origType'] == 'MARKET') & (sm_last_head['status'] == 'FILLED')].tail(1).index)
                limit02 = float(df_oh.loc[sm_last_head.tail(1).index]['executedQty'])
                market_added02 = round(float(df_oh.loc[market_head02]['executedQty']) + limit02)
                doubled02 = round(float(df_oh.loc[flat_ranges[ff]]['executedQty']) *2.23)
                if market_added02 != doubled02:
                    time = float(df_oh.loc[flat_ranges[ff]]['time']/1000)
                    formatted_time = datetime.datetime.fromtimestamp(time).strftime('%m%d%Y %H:%M:%S')
                    price = float(df_oh.loc[flat_ranges[ff]]['avgPrice'])
                    amount = float(df_oh.loc[flat_ranges[ff]]['executedQty'])
                    corrected_not_double = pd.DataFrame({'Symbol' :[tick], 'time':[formatted_time], 'amount':[amount], 'price':[price]})
                    not_corrected_tickers = not_corrected_tickers.append(corrected_not_double, ignore_index = True)
                    # st.write(corrected_not_double)
                else:
                    pass

        if len(flat_ranges) <= 2:
            pass
        else:
            subtract = []
            sm_next  = []
            # st.write(flat_ranges)
            last_sm_next = sym_check.index
            
            for dd in range(1, len(flat_ranges)-1):
                s = flat_ranges[dd+1] - flat_ranges[dd]
                subtract.append(s)
            
            for vi in  range(2, len(flat_ranges)):
                sm_next.append(flat_ranges[vi])
                
            # st.write(sm_next)  
            # st.write(subtract)
            
            sm_head01 = df_oh.loc[fr+1:flat_ranges[1]]
            # st.write(sm_head01)
            market_head01 = np.array(sm_head01[(sm_head01['origType'] == 'MARKET') & (sm_head01['status'] == 'FILLED')].tail(1).index)
            
            limit01 = sm_head01[(sm_head01['origType'] == 'LIMIT') & (sm_head01['status'] == 'FILLED') ].tail(1)['executedQty']
            market_added01 = round(float(df_oh.loc[market_head01]['executedQty']) + float(limit01))
            doubled01 = round(float(df_oh.loc[flat_ranges[1]]['executedQty']) *2.23)
            
            if market_added01 != doubled01:
                time = float(df_oh.loc[flat_ranges[1]]['time']/1000)
                formatted_time = datetime.datetime.fromtimestamp(time).strftime('%m%d%Y %H:%M:%S')
                price = float(df_oh.loc[flat_ranges[1]]['avgPrice'])
                amount = float(df_oh.loc[flat_ranges[1]]['executedQty'])
                corrected_not_double = pd.DataFrame({'Symbol' :[tick], 'time':[formatted_time], 'amount':[amount], 'price':[price]})
                not_corrected_tickers = not_corrected_tickers.append(corrected_not_double, ignore_index = True)
                # st.write(corrected_not_double)
            else:
                pass

            # index 3 to last of flat range
            for sm_i in range(min(len(sm_next), len(subtract))):
                sms_i = (sm_next[sm_i] - subtract[sm_i]) +1 
                sm_head02 = df_oh.loc[sms_i:sm_next[sm_i]]
                # st.write(sm_head02)
                market_head02 = np.array(sm_head02[(sm_head02['origType'] == 'MARKET') & (sm_head02['status'] == 'FILLED')].tail(1).index)
                limit02 = sm_head02[(sm_head02['origType'] == 'LIMIT') & (sm_head02['status'] == 'FILLED')  ].tail(1)['executedQty']
                market_added02 = round(float(df_oh.loc[market_head02]['executedQty']) + float(limit02))
                doubled02 = round(float(sm_head02.tail(1)['executedQty']) *2.23)
                if market_added02 != doubled02:
                    time = float(df_oh.loc[sm_head02.tail(1).index]['time']/1000)
                    formatted_time = datetime.datetime.fromtimestamp(time).strftime('%m%d%Y %H:%M:%S')
                    price = float(df_oh.loc[sm_head02.tail(1).index]['avgPrice'])
                    amount = float(df_oh.loc[sm_head02.tail(1).index]['executedQty'])
                    corrected_not_double = pd.DataFrame({'Symbol' :[tick], 'time':[formatted_time], 'amount':[amount], 'price':[price]})
                    not_corrected_tickers = not_corrected_tickers.append(corrected_not_double, ignore_index = True)
                    # st.write(corrected_not_double)
                else:
                    pass
            
                
            # sm_last_head = df_oh.loc[sm_next[-1]:last_sm_next[-1]]
            # st.write(sm_last_head)
st.write(not_corrected_tickers)
st.write(tickers_not_double)
 
# ################################ ~~~~ END OF ALL OF MARKET SEARCH IN STOP MARKET FIELDS ~~~~~ ################################################








