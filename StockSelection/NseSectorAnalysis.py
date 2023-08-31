import io
import nsepy as nse
import pandas as qw
import pymongo
import xlwings as xw
from pandas import unique

from utility import *
from NseStocksAnalysis import fetch_stocksdelvol, processdeliveryvolume, processstockfuturedata, narrowrange, \
    movingaverages
import configparser
import json
from datetime import datetime, timedelta, date
from pymongo import MongoClient
# Declaration of files and display setting for panda
config = configparser.RawConfigParser()
config.read('ConfigFile.properties')

path = config.get('filedetails', 'filelocation')
filename = config.get('filedetails', 'NseSectorAnalysis.filename')
excel_file = path + filename
print(excel_file)
wb = xw.Book(excel_file)
sheet_delvol = wb.sheets("DelVolData")
sheet_movingaverages = wb.sheets("movingaverages")
sheet_bolingerband = wb.sheets("bolingerband")
sheet_indices = wb.sheets("Sectors")
get_session_cookies()
dt = datetime.today()
sdt = datetime.today() - timedelta(days=300)
expiry = (LastThInMonth(dt.year, dt.month)).strftime('%d-%b-%Y')
startdate = (date(sdt.year, sdt.month, sdt.day))
# Todays date
enddate = (date(dt.year, dt.month, dt.day))

startdatefut = previousexpiry = LastThInMonth(dt.year, dt.month - 1)
startdatefut += timedelta(days=1)


enddatefut = date(dt.year, dt.month, dt.day)
million = 100000
pd.set_option('display.width', 1500)
pd.set_option('display.max_columns', 75)
pd.set_option('display.max_rows', 10000)

def indicesintraday():
    url = "https://www.nseindia.com/api/allIndices"
    headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/80.0.3987.100 Safari/537.36',
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br"}


    # cookie_dict ={'bm_sv':'399C35EBE5B33A2AE6157217AB14E791~+wbZmQSw55+ee6qWONJHs4smJK6UXtR1mldzOjtCKXZht8dUnmKBFXYgyRm'
    # 'E7dRIZgoUOxW9fUQ27xfGTnMa9mMwFPsADL9pi8caRNwRHQruYqhY5hQKB96AgSKghg0zyzAQT4IgynhMhCg3yMo1+LIFQv86PwwRgRc/AeMPgZM='}
    # cookie_dict = get_session_cookies()
    try:
        # cookie_dict = get_session_cookies()
        cookie_dict = json.loads(open('cookies').read())
    except Exception as error:
        print("Error reading cookies indicesintraday")
        cookie_dict = get_session_cookies()

    session = requests.session()
    for cookie in cookie_dict:
        if "bm_sv" in cookie or 'nseappid' in cookie or "nsit" in cookie:
            session.cookies.set(cookie, cookie_dict[cookie])
    try:
        r = session.get(url, headers=headers, verify=False).json()
    except Exception as error:
        print('error in reading cookies')
        for cookie in cookie_dict:
            if "bm_sv" in cookie or 'nseappid' in cookie or "nsit" in cookie:
                session.cookies.set(cookie, cookie_dict[cookie])
        r = session.get(url, headers=headers, verify=False).json()

    dfindices = pd.DataFrame(r['data'])
    dfindices = dfindices[dfindices['key'] == 'SECTORAL INDICES']

    lists = dfindices.indexSymbol.tolist()
    print(lists)

    for name in lists:
        dsets = dfindices[dfindices['indexSymbol'] == name]
        dp = qw.DataFrame(dsets)
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        dg = client["SectorAnalysis"]
        stocks = dg[name]
        data = dp
        data_dict = data.to_dict("records")
        stocks.insert_many(data_dict)


    # print("Holidays " + holidays())
    # if dt in lastworkday():
    #    dfindices['date'] = dt

    print('printing index details')
    print(dfindices)
    dr = qw.DataFrame(dfindices)

    # dp = qw.DataFrame(dsets)
    # client = pymongo.MongoClient("mongodb://localhost:27017/")
    # dg = client["SectorAnalysis"]
    # stocks = dg[name]
    # data = dp
    # data_dict = data.to_dict("records")
    # stocks.insert_one(data_dict)

    # inserting (sample) data into MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    dg = client["SectorAnalysis"]
    stocks = dg["Stocks"]
    data = dr
    data_dict = data.to_dict("records")
    stocks.insert_many(data_dict)

    # pulling data back from MongoDB
    # data_from_dg = stocks.find_one({"index": "NIFTY BANK"})
    # pf = pd.DataFrame(data_from_dg["data"])
    # pf.set_index("Date", inplace=True)
    # print(pf)
    # remove hyperlink

    # indiceshistory(dfindices.indexSymbol)

    dfindices = dfindices.filter(
        ['index', 'indexSymbol', 'last', 'open', 'high', 'low', 'previousClose', 'variation', 'percentChange',
         'declines', 'advances',
         'perChange365d', 'perChange30d'])

    # info = dfindices.to_dict(orient='records')
    # db = client["Sectors"]
    # db.Stocks.insert_many(info)
    return dfindices


def indices_history(index):
    for idx in index.indexSymbol:
        url = "https://www1.nseindia.com/products/dynaContent/equities/indices/historicalindices.jsp?indexType=" + idx + "&fromDate=01-05-2021&toDate=13-08-2021"
        print("URL = " + url)

def indicesswing(indicesList):
    df = qw.DataFrame()
    print(df)
    for single_date in daterange(startdate, enddate):
        try:
            if single_date.weekday() not in weekdays:
                datestring = single_date.strftime("%d%m%Y")
                # print(datestring)
                if datestring in holidays():
                    continue
                url = "https://www1.nseindia.com/content/indices/ind_close_all_" + datestring + ".csv"
                r = requests.get(url=url).content
                df_sectors = pd.read_csv(io.StringIO(r.decode('utf8')), skiprows=0)
                df_sectors.columns = df_sectors.columns.str.replace(' ', '_')
                df_sectors['Index_Name'] = df_sectors.Index_Name.str.upper()
                df_sectors = df_sectors[df_sectors['Index_Name'].isin(indicesList)]
                df_sectors.reset_index(inplace=True, drop=True)
                df = pd.concat([df, df_sectors])
                df.drop_duplicates(inplace=True)
                df.reset_index(inplace=True, drop=True)
        except Exception as error:
            print("error {0}".format(error))
            continue
    return df
# try catch to be added to missing present date data before 6:30 PM (A fixed time)


def sectorswisestocks(index):
    url = "https://www.nseindia.com/api/equity-stockIndices?index=" + index
    headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/80.0.3987.100 Safari/537.36',
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.nseindia.com/market-data/live-equity-market?symbol=NIFTY%20METAL"}

    try:
        # cookie_dict = get_session_cookies()
        cookie_dict = json.loads(open('cookies').read())
    except Exception as error:
        print("Error reading cookies indicesintraday")
        cookie_dict = get_session_cookies()

    session = requests.session()

    for cookie in cookie_dict:
        if "bm_sv" in cookie or 'nseappid' in cookie or "nsit" in cookie:
            session.cookies.set(cookie, cookie_dict[cookie])
    try:
        r = session.get(url, headers=headers, verify=False).json()
    except Exception as error:
        print('error in reading cookies')
        for cookie in cookie_dict:
            if "bm_sv" in cookie or 'nseappid' in cookie or "nsit" in cookie:
                session.cookies.set(cookie, cookie_dict[cookie])
        r = session.get(url, headers=headers, verify=False).json()
    print(type(r))
    # while True:
    #     try:
    #         if (r.status_code != 200):
    #             time.sleep(5)
    #             print("Checking Response Status Code again sectorswisestocks", r.status_code)
    #             r = requests.get(url, headers=headers, verify=False)
    #         if (r.status_code == 200):
    #             print('status code 200 sectorswisestocks:::::', r.status_code)
    #             r = r.json()
    #             break
    #     except Exception as error:
    #         print("Error in main. Error : {0} sectorswisestocks".format(error))
    #         continue
    print(r)
    df = pd.DataFrame(r['data'])
    df = df.filter(['symbol', 'open', 'dayHigh', 'dayLow', 'lastPrice', 'previousClose', 'pChange', 'perChange365d',
                    'perChange30d'])
    df['Index'] = index

    return df

def topperformingsector(indexdf, duration):
    IndexList = indexdf['Index_Name'].values.tolist()
    IndexList = list(dict.fromkeys(IndexList))
    for data in IndexList:
        indexdf['7Close'] = round((indexdf['Closing_Index_Value'].rolling(7).sum()), 2)
        indexdf['7Closechange'] = round((indexdf['Points_Change'].rolling(7).sum()), 2)
        indexdf['perChange7D'] = round((indexdf['7Closechange'] / indexdf['7Close']) * 100, 2)
        indexdf['15Close'] = round((indexdf['Closing_Index_Value'].rolling(15).sum()), 2)
        indexdf['15Closechange'] = round((indexdf['Points_Change'].rolling(15).sum()), 2)
        indexdf['perChange15D'] = round((indexdf['15Closechange'] / indexdf['15Close']) * 100, 2)


# funciton defination to fetch delivery volume data from nse using NsePy method get history
def main():
    # Dataframe to gather all stocks with sectors

    dfSectorStocks = pd.DataFrame()

    # getting indices list from function intraday which will return active sectorial indices
    indicesList = indicesintraday()['index']
    indicesList = indicesList.values.tolist()

    # function will return sector data for given range
    dfindices = indicesswing(indicesList)
    topperformingsector(dfindices, duration=7)

    # function will return stock data for given range

    for index in indicesList:
        df = sectorswisestocks(index)

        dfSectorStocks = pd.concat([dfSectorStocks, df])

    dfSectorStocks = dfSectorStocks[dfSectorStocks['symbol'] != dfSectorStocks['Index']]
    dfSectorStocks = dfSectorStocks.reset_index(drop=True).drop_duplicates(subset=['symbol'],
                                                                           keep='first')
    SectorStocksList = dfSectorStocks['symbol'].tolist()
    dfSectorStocks.rename(columns={'symbol': 'Symbol'}, inplace=True)

    # df_futures= processstockfuturedata(dfSectorStocks,startdatefut,enddatefut)
    #
    # df_futures.reset_index(level=0, inplace=True, col_level=1,drop=True)
    # print('Printing stocks future fetched data..')

    # dfSectorStocks.reset_index(inplace=True, drop=True)

    # Fetching delivery volumne data for stocks ######

    dfstocks = fetch_stocksdelvol(SectorStocksList, startdate, enddate)
    print('printing startdate', startdate)
    dfstocks.reset_index(inplace=True)

    # dfstocks.reset_index(level=0, inplace=True, col_level=1,drop=True)

    dfstocks = pd.merge(dfstocks, dfSectorStocks.filter(['Symbol', 'perChange365d', 'Index']), on='Symbol')
    print('###########Printing sector stocks')
    print(max(dfstocks['Date']))

    # processing  delivery volumne data for stocks for increasin and decreasing high delivery,volume ,value percentage
    #

    df = processdeliveryvolume(dfstocks, SectorStocksList)

    # processing  narrow range data for stocks
    dfnr = narrowrange(dfstocks)

    # processing moving averages data for stocks
    dfma = movingaverages(dfstocks)

    # processing futures  data for stocks
    #  print(df)

    df.reset_index(level=0, inplace=True, col_level=1)

    dfindicesintraday = indicesintraday()

    print(dfindicesintraday)

    df.set_index(['Date', 'Symbol'], inplace=True)
    dfindices.reset_index(level=0, inplace=True, col_level=1)
    dfindices.set_index(['Index_Date', 'Index_Name'], drop=True, inplace=True)

    # Filtering data for excel sheets
    dfdelvol = df.filter(
        ['Date', 'Symbol', 'Index', 'Close', 'VWAP', 'Volume', 'Turnover', '3Turnover', '5Turnover', '8Turnover',
         '13Turnover', 'Trades', 'DeliverableVolume', '%Deliverble', 'perChange365d', 'Value', '3Value', '5Value',
         '8Value',
         '13Value', 'Val%', '3DeliveryQty', '3Volume', '5DeliveryQty', '5Volume', '8DeliveryQty',
         '8Volume', '	13DeliveryQty', '13Volume', '3AvgDel%', '5AvgDel%', '8AvgDel%', '13AvgDel%', 'deldirection',
         'TQ/NT', '3TQ/NT', '5TQ/NT', '8TQ/NT', '13TQ/NT', '3Trades', '5Trades', '8Trades', '13Trades', '3Close',
         '5Close',
         '8Close', '13Close', 'perPrice', 'CashMoneyFlow', 'cf7sum', 'turnoverdirection', 'valdirection',
         'deldirection', 'pricedirection'])

    dfma = dfma.filter(['Date', 'Symbol', 'Index', 'Close', 'MA20', 'MA50', 'MA100',
                        'MA200', 'MA20-50', 'per20MA', 'MA20', 'CashMoneyFlow', 'cf7sum', 'perPrice', 'pricedirection'])

    dfbolingerband = dfnr.filter(
        ['Date', 'Symbol', 'Index', 'Close', 'per20MA', 'MA20', '20dSTD', 'Upper', 'Lower', 'perPrice', 'P5',
         'P3', 'pricedirection'])

    # Filtering data for excel sheets ends here

    if len(dfindices) > 0:
        sheet_indices.range("A1").options(index=True, headers=True).value = dfindices[::-1]
    if len(df) > 0:
        # datareturn.set_index('Date',inplace=True)
        sheet_delvol.range('2:50000').clear_contents()
        sheet_movingaverages.range('2:50000').clear_contents()
        sheet_bolingerband.range('2:50000').clear_contents()
        sheet_delvol.range("A1").options(index=True, headers=True).value = dfdelvol.sort_values(
            ['Date', 'Index', 'Val%', 'perPrice'], ascending=[0, 1, 0, 0])
        sheet_movingaverages.range("A1").options(index=True, headers=True).value = dfma.sort_values(
            ['Date', 'Index', 'MA20-50', 'perPrice'], ascending=[0, 1, 1, 0])
        sheet_bolingerband.range("A1").options(index=True, headers=True).value = dfbolingerband.sort_values(
            ['Date', 'Index', 'P3', 'P5'], ascending=[0, 1, 1, 1])
        print("Del vol data updated")


if __name__ == '__main__':
    main()
