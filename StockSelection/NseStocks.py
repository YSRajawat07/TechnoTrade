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
from dateutil.relativedelta import relativedelta

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
# get_session_cookies()
dt = datetime.today()
sdt = datetime.today() - timedelta(days=300)
expiry = (LastThInMonth(dt.year, dt.month)).strftime('%d-%b-%Y')
startdate = (date(sdt.year, sdt.month, sdt.day))
# Todays date
enddate = (date(dt.year, dt.month, dt.day))

if dt.month == 1:
    startdatefut = previousexpiry = LastThInMonth(dt.year-1, 12)
else:
    startdatefut = previousexpiry = LastThInMonth(dt.year, dt.month - 1)

startdatefut += timedelta(days=1)

enddatefut = date(dt.year, dt.month, dt.day)

million = 100000
pd.set_option('display.width', 1500)
pd.set_option('display.max_columns', 75)
pd.set_option('display.max_rows', 10000)

headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/80.0.3987.100 Safari/537.36',
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.nseindia.com/market-data/live-equity-market"
              }


def indicesintraday():
    url = "https://www.nseindia.com/api/allIndices"

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
        print("test1.0")
        print(url)
        r = session.get(url, headers=headers, verify=False).json()
        print("test2.0")
    except Exception as error:
        print('error in reading cookies')
        for cookie in cookie_dict:
            if "bm_sv" in cookie or 'nseappid' in cookie or "nsit" in cookie:
                session.cookies.set(cookie, cookie_dict[cookie])
        r = session.get(url, headers=headers, verify=False).json()

    dfindices = pd.DataFrame(r['data'])
    # print("Printing dfindices")
    # print(dfindices)
    dfindices = dfindices[dfindices['key'] == 'SECTORAL INDICES']
    lists = dfindices.indexSymbol.tolist()
    print(lists)

    for sector in lists:
        print(sector)
        stocksindices(sector)

    return dfindices

def stocksindices(sector):
    res = len(sector.split())
    str = ""
    for i in range(res):
        if sector.split(' ')[i] == "&":
            str = str + "%26%20"
        else:
            str = str + sector.split(' ')[i] + "%20"
    str = str[:-3]
    url1 = "https://www.nseindia.com/api/equity-stockIndices?index=" + str
    # url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20BANK"

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

        r = session.get(url1, headers=headers, verify=False).json()
    except Exception as error:
        print('error in reading cookies chotu')
        for cookie in cookie_dict:
            if "bm_sv" in cookie or 'nseappid' in cookie or "nsit" in cookie:
                session.cookies.set(cookie, cookie_dict[cookie])
                continue
        print('test A')
        print(url1)
        r = session.get(url1, headers=headers, verify=False).json()

    dsindices = pd.DataFrame(r['data'])
    print("Printing dsindices")
    print(dsindices)
    lists1 = dsindices.symbol.tolist()
    print(lists1)
    str1 = ""
    for i in range(res):
        str1 = str1 + sector.split(' ')[i] + "_"

    str1 = str1[:-1]
    for name in lists1:
        dsets = dsindices[dsindices['symbol'] == name]
        dp = qw.DataFrame(dsets)
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        dg = client[str1]
        stocks = dg[name]
        data = dp
        data_dict = data.to_dict("records")
        stocks.insert_many(data_dict)


dfg = indicesintraday()
