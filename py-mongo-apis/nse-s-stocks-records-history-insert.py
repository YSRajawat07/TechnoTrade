from datetime import datetime, date
from operator import itemgetter
from datetime import datetime, timedelta, date
import pandas as pd
import pymongo
import xlwings as xw
import nsepy as nse
import numpy as np
from nsepy import get_history
from StockSelection.utility import *
import pandas_ta as ta
import configparser
# Declaration of files and display setting for panda

config = configparser.RawConfigParser()
config.read('ConfigFile.properties')



dt = datetime.today()
month = dt.month

#capture current dates
dt = datetime.today()
#canculate the start date from the today's date by reducing the no of days to fetch the stocks data history
#sdt = datetime.today() - timedelta(days=600)
sdt = datetime.today()

#get last thrusday of everymonth as an expiry date
expiry = (LastThInMonth(dt.year, dt.month)).strftime('%d-%b-%Y')

#converting into the date fromat for passing as parameter into get history method of nse py
startdate = (date(sdt.year, sdt.month, sdt.day))

# Todays date
enddate = (date(dt.year, dt.month, dt.day))

million = 100000
pd.set_option('display.width', 1500)
pd.set_option('display.max_columns', 75)
pd.set_option('display.max_rows', 10000)

#MongoDB connections
client = pymongo.MongoClient("mongodb://localhost:27017/")
niftydb = client["NiftyDB"]
print('DB connection established')

def fnolist():
    url = "https://www1.nseindia.com/live_market/dynaContent/live_watch/stock_watch/foSecStockWatch.json"

    headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/80.0.3987.100 Safari/537.36',
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www1.nseindia.com/live_market/dynaContent/live_watch/equities_stock_watch.htm"}
    r = requests.get(url, headers=headers).json()
    fno = [data['symbol'] for data in r['data']]

    return fno


# funciton defination to fetch delivery volume data from nse using NsePy method get history

def stocks_delvol_insert(stockslist, startDate, endDate):
    print('Fetching stocks delivery volume data..........')
    df = pd.DataFrame()

    for symbol in stockslist:
        try:
           print('fetching data for:::: ',symbol)
           df = pd.concat([df, nse.get_history(symbol=symbol, start=startDate, end=endDate)])
           print('fetching data for:::: ',symbol,' end##########')

        except Exception as error:
            print('before error')
            print("fetch_stocksdelvol error {0}".format(error))
            continue

   # df['Date'] = df.index
    df['Date'] = df.index
    df['Date']= df['Date'].astype(str)
    print(df.to_dict('records'))
    niftydb.nse_stocks_historical_data.create_index([('Symbol', pymongo.DESCENDING), ('Date', pymongo.DESCENDING)], unique=True)
    niftydb.nse_stocks_historical_data.insert_many(df.to_dict('records'))
    print('.......................Stocks data update completed....................')
    return df


# Append the stock analysis sheets for daily data



def stock_futures(df, expdate,startdatefut,enddatefut):
    fullrefreshfull = 'yes'
    stock_fut = pd.DataFrame()
    if fullrefreshfull == 'yes':
        for symbol in df['Symbol'].tolist():
            try:

                stock_fut = pd.concat([stock_fut, get_history(symbol, start=startdatefut, end=enddatefut, futures=True,
                                                            expiry_date=expdate)])


            except Exception as error:
                print('before error')
                print("stock_fut fetching historical data error {0}".format(error))
                continue

        print('stock_futures: stock_futures end')
    elif fullrefreshfull == 'no':

        yesterday = date.today() - timedelta(days=1)
        stock_fut1 = pd.DataFrame()
        for symbol in df['symbol'].tolist():
            try:
                stock_fut1 = pd.concat([stock_fut, get_history(symbol, start=yesterday,
                                                               end=yesterday,
                                                               futures=True, expiry_date=expdate)])
            except Exception as error:
                print("stock_fut error {0}".format(error))
                continue

        stock_fut = append_xl(stock_fut1, excel_file, 'Futures', 'Date')

        print(stock_fut)

    return stock_fut


def processstockfuturedata(df,startdatefut,enddatefut):
    print('start pulling FnO list')
    stocksList = fnolist()
    dffutures =df[df['Symbol'].isin(stocksList)]
    dffutures =dffutures.filter(['Symbol','Index','close'])
    print('FnO list Completed')
    dffut= pd.DataFrame()

    for month in range(dt.month, dt.month+1):

        expdate = LastThInMonth(dt.year, month)
        print('fetching data for month,expiry :',month,expdate)
        dffut = dffut.append(stock_futures(dffutures, expdate,startdatefut,enddatefut))
    dffut.reset_index(inplace=True,drop=True)

    print(dffut)


    fut_sumoi = dffut.groupby(['Date', 'Symbol'])['Open Interest'].sum()
    futsumchoi = dffut.groupby(['Date', 'Symbol'])['Change in OI'].sum()

    fut_sumoi = pd.DataFrame(fut_sumoi)
    futsumchoi = pd.DataFrame(futsumchoi)
    fut_sumoi = fut_sumoi.join(futsumchoi)

    # fut_sumoi.reset_index(inplace=True)

    fut_sumoi.reset_index(level=1, inplace=True, col_level=1)

    # df.drop(columns=['Open Interest', 'Change in OI'],inplace=True)
    dffut.set_index(['Date', 'Symbol'], inplace=True)
    fut_sumoi.set_index(['Date', 'Symbol'], inplace=True)


    dffutures = dffutures.join(fut_sumoi, rsuffix="_Sum")



    df = df[df['Expiry'] == LastThInMonth(dt.year, month)]
    try:
        df['OI%'] = (df['Change in OI_Sum'].divide((df['Open Interest_Sum']) - df['Change in OI_Sum'])) * 100

    except Exception as error:
        print(" processstockfuturedata: error {0}".format(error))

    if len(df) > 0:
        df['Long Buildup'] = np.where(((df['OI%'] > 0) & (df['price%'] > 0)), 'True','False')
        df['Short Buildup'] = np.where(((df['OI%'] > 0) & (df['price%'] < 0)), 'True','False')
        df['Long unwinding '] = np.where(((df['OI%'] < 0) & (df['price%'] < 0)), 'True','False')
        df['Short covering'] = np.where(((df['OI%'] < 0) & (df['price%'] > 0)), 'True','False')

        df['Direction'] = np.where((df['Long Buildup'] == 'True'), 'Long Buildup',
                                            (np.where((df['Short Buildup'] == 'True'), 'Short Buildup',
                                                      (np.where((df['Long unwinding '] == 'True'),
                                                                'Long unwinding',
                                                                (np.where((df['Short covering'] == 'True'),
                                                                          'Short covering', 'Nothing')))))))
        df['Turnover_cash'] = df['Turnover_cash'] / million
        df['Volume'] = df['Volume'] / million
        return df
    else:
        print("processstockfuturedata :empty dataframe for futures, nothing to update")

def main():
    stockslist = fnolist()
    stocks_delvol_insert(stockslist, startdate, enddate)

if __name__ == '__main__':
    main()
