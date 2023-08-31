import ta as ta
import xlwings as xw
import nsepy as nse
import numpy as np
from nsepy import get_history
from utility import *
# import pandas_ta as ta
import configparser
from datetime import datetime, timedelta, date
# Declaration of files and display setting for panda

config = configparser.RawConfigParser()
config.read('ConfigFile.properties')

path = config.get('filedetails', 'filelocation')
filename = config.get('filedetails', 'NseStocksAnalysis.filename')
excel_file = path + filename
print(excel_file)

# Declaration of files and display setting for panda
wb = xw.Book(excel_file)

sheet_futures = wb.sheets("Futures")
sheet_combined = wb.sheets("Combined")


dt = datetime.today()
month = dt.month

million = 100000
pd.set_option('display.width', 1500)
pd.set_option('display.max_columns', 75)
pd.set_option('display.max_rows', 10000)


def fnolist():
    url = "https://www1.nseindia.com/live_market/dynaContent/live_watch/stock_watch/foSecStockWatch.json"

    headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/80.0.3987.100 Safari/537.36',
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www1.nseindia.com/live_market/dynaContent/live_watch/equities_stock_watch.htm"}
    r = requests.get(url, headers=headers).json()

    stocksymbols = [data['symbol'] for data in r['data']]
    return stocksymbols

# funciton defination to fetch delivery volume data from nse using NsePy method get history

def fetch_stocksdelvol(stockslist, startDate, endDate):
    print('Fetching stocks delivery volume data..........')
    df = pd.DataFrame()

    for symbol in stockslist:
        try:
            print('fetching data for:::: ',symbol)
            df = pd.concat([df, nse.get_history(symbol=symbol, start=startDate, end=endDate)])
            print('fetching data for:::: ', symbol, ' end##########')

        except Exception as error:
            print('before error')
            print("fetch_stocksdelvol error {0}".format(error))
            continue
    print('.......................End....................')
    return df


# Append the stock analysis sheets for daily data



def stock_futures(df, expdate, startdatefut, enddatefut):
    fullrefreshfull = 'yes'
    stock_fut = pd.DataFrame()
    if fullrefreshfull == 'yes':
        for symbol in df['Symbol'].tolist():
            try:
                stock_fut = pd.concat([stock_fut, get_history(symbol, start=startdatefut, end=enddatefut, futures=True, expiry_date = expdate)])
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

def movingaverages(df):
    print('movingaverages: processing for moving averages started')
    df['CashMoneyFlow'] = np.where(
        (df.Close.astype(float) > df.PrevClose.astype(float)),
        round(((df.DeliverableVolume * df.VWAP) / 1000000).astype(float), 2),
        -round(((df.DeliverableVolume * df.VWAP) / 1000000).astype(float), 2))
    # cash flow for last 7 days
    df['cf7sum'] = df.CashMoneyFlow.rolling(7).sum()

    df['MA5'] = round(((df.Close.rolling(window=5).mean())/df.Close)*100, 2)
    df['MA8'] = round(((df.Close.rolling(window=8).mean())/df.Close)*100, 2)
    df['MA13'] = round(((df.Close.rolling(window=13).mean())/df.Close)*100, 2)
    df['MA20'] = round(((df.Close.rolling(window=20).mean())/df.Close)*100, 2)
    df['MA50'] = round(((df.Close.rolling(window=50).mean())/df.Close)*100, 2)
    df['MA100'] = round(((df.Close.rolling(window=100).mean())/df.Close)*100, 2)
    df['MA200'] = round(((df.Close.rolling(window=200).mean())/df.Close)*100, 2)

    df['MA20-50'] = df['MA20'] - df['MA50']
    # price percentage above 20 ema
    df['per20MA'] = (df['Close'] / df['MA20']) * 100

    df['20dSTD'] = df.Close.rolling(window=20).std()
    df['Upper'] = df['MA20'] + (df['20dSTD'] * 2)
    df['Lower'] = df['MA20'] - (df['20dSTD'] * 2)
    df['perPrice'] = round(
        (((df['Close'] - df['PrevClose']) / df['PrevClose']) * 100), 2)

    df['pricedirection'] = np.where((((df['3Close'] >= df['5Close']) &
                                              (df['5Close'] >= df['8Close'])) &
                                             (df['8Close'] >= df['13Close'])), 'Increasing',
                                            (np.where((((df['3Close'] <= df['5Close']) &
                                                        (df['5Close'] <= df['8Close'])) &
                                                       (df['8Close'] <= df['13Close'])),
                                                      'Decreasing', 'Nothing')))

    print('movingaverages: processing for moving averages ended')

    return df

def narrowrange(df):
    print('narrowrange: processing for narrow range started')
    df.columns = df.columns.str.replace(' ', '')

    df['CashMoneyFlow'] = np.where(
        (df.Close.astype(float) > df.PrevClose.astype(float)),
        round(((df.DeliverableVolume * df.VWAP) / 1000000).astype(float), 2),
        -round(((df.DeliverableVolume * df.VWAP) / 1000000).astype(float), 2))
    # cash flow for last 7 days
    df['cf7sum'] = df.CashMoneyFlow.rolling(7).sum()
    df['3Close'] = round((df['Close'].rolling(3).mean()), 2)
    df['5Close'] = round((df['Close'].rolling(5).mean()), 2)
    df['8Close'] = round((df['Close'].rolling(8).mean()), 2)
    df['13Close'] = round((df['Close'].rolling(13).mean()), 2)
    df['pricedirection'] = np.where((((df['3Close'] >= df['5Close']) &
                                              (df['5Close'] >= df['8Close'])) &
                                             (df['8Close'] >= df['13Close'])), 'Increasing',
                                            (np.where((((df['3Close'] <= df['5Close']) &
                                                        (df['5Close'] <= df['8Close'])) &
                                                       (df['8Close'] <= df['13Close'])),
                                                      'Decreasing', 'Nothing')))


    df['ATP'] = round(((df['Close'] + df['High'] + df['Low']) / 3), 2)

    # datareturn['Vol%'] = (datareturn['Volume'] / datareturn['AvgVol'])

    df['perPrice'] = round(
        (((df['Close'] - df['PrevClose']) / df['PrevClose']) * 100), 2)
    df['P5'] = (df['Close'] - df['5Close'])
    df['P3'] = (df['Close'] - df['3Close'])

    print('narrowrange: processing for narow range ended')

    return df

def processdeliveryvolume(datareturn, list):

        print('processdeliveryvolume: processing for delivery volumne started')
        datareturn.columns = datareturn.columns.str.replace(' ', '')

        for data in list:
            try:

                datareturn['3Close'] = round((datareturn['Close'].rolling(3).mean()), 2)
                datareturn['5Close'] = round((datareturn['Close'].rolling(5).mean()), 2)
                datareturn['8Close'] = round((datareturn['Close'].rolling(8).mean()), 2)
                datareturn['13Close'] = round((datareturn['Close'].rolling(13).mean()), 2)

                datareturn['Value'] = datareturn.Volume * datareturn.Close

                datareturn['3Value'] = datareturn.Value.rolling(3).mean() / million
                datareturn['5Value'] = datareturn.Value.rolling(5).mean() / million
                datareturn['8Value'] = datareturn.Value.rolling(8).mean() / million
                datareturn['13Value'] = datareturn.Value.rolling(13).mean() / million

                datareturn['Value'] = datareturn['Value'] / million

                datareturn['Val%'] = (datareturn['Value'] / datareturn['3Value']) * 100

                datareturn['3DeliveryQty'] = datareturn.DeliverableVolume.rolling(3).sum()
                datareturn['3Volume'] = datareturn.Volume.rolling(3).sum()

                datareturn['5DeliveryQty'] = datareturn.DeliverableVolume.rolling(5).sum()
                datareturn['5Volume'] = datareturn.Volume.rolling(5).sum()

                datareturn['8DeliveryQty'] = datareturn.DeliverableVolume.rolling(8).sum()
                datareturn['8Volume'] = datareturn.Volume.rolling(8).sum()

                datareturn['13DeliveryQty'] = datareturn.DeliverableVolume.rolling(13).sum()
                datareturn['13Volume'] = datareturn.Volume.rolling(13).sum()

                datareturn['3AvgDel%'] = (datareturn['3DeliveryQty'] / datareturn['3Volume']) * 100
                datareturn['5AvgDel%'] = (datareturn['5DeliveryQty'] / datareturn['5Volume']) * 100
                datareturn['8AvgDel%'] = (datareturn['8DeliveryQty'] / datareturn['8Volume']) * 100
                datareturn['13AvgDel%'] = (datareturn['13DeliveryQty'] / datareturn['13Volume']) * 100

                datareturn['3AvgDel%'] = datareturn['3AvgDel%'].astype(float)
                datareturn['5AvgDel%'] = datareturn['5AvgDel%'].astype(float)
                datareturn['8AvgDel%'] = datareturn['8AvgDel%'].astype(float)
                datareturn['13AvgDel%'] = datareturn['13AvgDel%'].astype(float)

                datareturn['deldirection'] = np.where((((datareturn['3AvgDel%'] >= datareturn['5AvgDel%']) &
                                                        (datareturn['5AvgDel%'] >= datareturn['8AvgDel%'])) &
                                                       (datareturn['8AvgDel%'] >= datareturn['13AvgDel%'])),
                                                      'Increasing',
                                                      (np.where((((datareturn['3AvgDel%'] <= datareturn['5AvgDel%']) &
                                                                  (datareturn['5AvgDel%'] <= datareturn['8AvgDel%'])) &
                                                                 (datareturn['8AvgDel%'] <= datareturn['13AvgDel%'])),
                                                                'Decreasing', 'Nothing')))

                # datareturn['deldirection'] = np.where((datareturn['3AvgDel%'] >= datareturn['5AvgDel%']), 'Increasing',
                #                                       (np.where((datareturn['3AvgDel%'] <= datareturn['5AvgDel%']),
                #                                                 'Decreasing', 'Nothing')))

                # QT/NT#####

                datareturn['TQ/NT'] = datareturn.DeliverableVolume / datareturn.Trades
                datareturn['3TQ/NT'] = datareturn.DeliverableVolume.rolling(3).sum() / datareturn.Trades.rolling(
                    3).sum()
                datareturn['5TQ/NT'] = datareturn.DeliverableVolume.rolling(5).sum() / datareturn.Trades.rolling(
                    5).sum()
                datareturn['8TQ/NT'] = datareturn.DeliverableVolume.rolling(8).sum() / datareturn.Trades.rolling(
                    8).sum()
                datareturn['13TQ/NT'] = datareturn.DeliverableVolume.rolling(13).sum() / datareturn.Trades.rolling(
                    13).sum()

                datareturn['3DeliveryQty'] = datareturn.DeliverableVolume.rolling(3).mean()
                datareturn['3Volume'] = datareturn.Volume.rolling(3).mean()

                datareturn['5DeliveryQty'] = datareturn.DeliverableVolume.rolling(5).mean()
                datareturn['5Volume'] = datareturn.Volume.rolling(5).mean()

                datareturn['8DeliveryQty'] = datareturn.DeliverableVolume.rolling(8).mean()
                datareturn['8Volume'] = datareturn.Volume.rolling(8).mean()

                datareturn['13DeliveryQty'] = datareturn.DeliverableVolume.rolling(13).mean()
                datareturn['13Volume'] = datareturn.Volume.rolling(13).mean()

                datareturn['3Trades'] = datareturn.Trades.rolling(3).mean()
                datareturn['5Trades'] = datareturn.Trades.rolling(5).mean()
                datareturn['8Trades'] = datareturn.Trades.rolling(8).mean()
                datareturn['13Trades'] = datareturn.Trades.rolling(13).mean()

                datareturn['3Turnover'] = round((datareturn.Turnover.rolling(3).mean() / million), 2)
                datareturn['5Turnover'] = round((datareturn.Turnover.rolling(5).mean() / million), 2)
                datareturn['8Turnover'] = round((datareturn.Turnover.rolling(8).mean() / million), 2)
                datareturn['13Turnover'] = round((datareturn.Turnover.rolling(13).mean() / million), 2)
                # datareturn['Turnover'] =datareturn['Turnover'] / million

                datareturn['turnoverdirection'] = np.where((((datareturn['3Turnover'] >= datareturn['5Turnover']) &
                                                             (datareturn['5Turnover'] >= datareturn['8Turnover'])) &
                                                            (datareturn['8Turnover'] >= datareturn['13Turnover'])),
                                                           'Increasing',
                                                           (np.where(
                                                               (((datareturn['3Turnover'] <= datareturn['5Turnover']) &
                                                                 (datareturn['5Turnover'] <= datareturn['8Turnover'])) &
                                                                (datareturn['8Turnover'] <= datareturn['13Turnover'])),
                                                               'Decreasing', 'Nothing')))

                datareturn['3Volume'] = round((datareturn['3Volume'] / million), 2)
                datareturn['5Volume'] = round((datareturn['5Volume'] / million), 2)
                datareturn['8Volume'] = round((datareturn['8Volume'] / million), 2)
                datareturn['13Volume'] = round((datareturn['13Volume'] / million), 2)
                # datareturn['Volume'] = round((datareturn['Volume']/million),2)

                datareturn['valdirection'] = np.where((((datareturn['3Value'] >= datareturn['5Value']) &
                                                        (datareturn['5Value'] >= datareturn['8Value'])) &
                                                       (datareturn['8Value'] >= datareturn['13Value'])), 'Increasing',
                                                      (np.where((((datareturn['3Value'] <= datareturn['5Value']) &
                                                                  (datareturn['5Value'] <= datareturn['8Value'])) &
                                                                 (datareturn['8Value'] <= datareturn['13Value'])),
                                                                'Decreasing', 'Nothing')))

                datareturn['pricedirection'] = np.where((((datareturn['3Close'] >= datareturn['5Close']) &
                                                  (datareturn['5Close'] >= datareturn['8Close'])) &
                                                 (datareturn['8Close'] >= datareturn['13Close'])), 'Increasing',
                                                (np.where((((datareturn['3Close'] <= datareturn['5Close']) &
                                                            (datareturn['5Close'] <= datareturn['8Close'])) &
                                                           (datareturn['8Close'] <= datareturn['13Close'])),
                                                          'Decreasing', 'Nothing')))

                # datareturn['valdirection'] = np.where((datareturn['3Value'] >= datareturn['5Value']),'Increasing',
                #                                       (np.where((datareturn['3Value'] <= datareturn['5Value']),'Decreasing','Nothing')))

                datareturn['CashMoneyFlow'] = np.where(
                    (datareturn.Close.astype(float) > datareturn.PrevClose.astype(float)),
                    round(((datareturn.DeliverableVolume * datareturn.VWAP) / 1000000).astype(float), 2),
                    -round(((datareturn.DeliverableVolume * datareturn.VWAP) / 1000000).astype(float), 2))
                # cash flow for last 7 days
                datareturn['cf7sum'] = datareturn.CashMoneyFlow.rolling(7).sum()

                datareturn['perPrice'] = round(
                    (((datareturn['Close'] - datareturn['PrevClose']) / datareturn['PrevClose']) * 100), 2)



            except Exception as error:
                print("processdeliveryvolume error {0}".format(error))
                continue

        print('processdeliveryvolume: processing for delivery ended')
        return datareturn


def processstockfuturedata(df,startdatefut,enddatefut):
    print('start pulling FnO list')
    stocksList = fnolist()
    dffutures =df[df['Symbol'].isin(stocksList)]
    dffutures =dffutures.filter(['Symbol','Index','close'])
    print('FnO list Completed')
    dffut= pd.DataFrame()

    for month in range(dt.month, dt.month+1):

        expdate = LastThInMonth(dt.year, month)
        print('fetching data for month,expiry :', month, expdate)
        dffut = dffut.append(stock_futures(dffutures, expdate, startdatefut, enddatefut))
    dffut.reset_index(inplace=True, drop=True)

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
