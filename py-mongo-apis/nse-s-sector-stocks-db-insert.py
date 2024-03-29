import io
import xlwings as xw
from utility import *
import configparser
import json
from datetime import datetime, timedelta, date
import pymongo
from pymongo.errors import BulkWriteError
import warnings
warnings.filterwarnings('ignore')

# Declaration of files and display setting for panda

# config = configparser.RawConfigParser()
# config.read('ConfigFile.properties')

# path = config.get('filedetails', 'filelocation')
# filename = config.get('filedetails', 'NseSectorAnalysis.filename')
#get_session_cookies()
dt = datetime.today()
#sdt = datetime.today() - timedelta(days=300)
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

#MongoDB connections
client = pymongo.MongoClient("mongodb://localhost:27017/")
niftydb = client["NiftyDB"]
print('DB connection established')


def sectors_daily_insert():
    url = "https://www.nseindia.com/api/allIndices"

    headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/80.0.3987.100 Safari/537.36',
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br"}

    try:
        # cookie_dict = get_session_cookies()
        cookie_dict = json.loads(open('../StockSelection/cookies').read())
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

    try:
        dfindices = pd.DataFrame(r['data'])
        dfindices = dfindices[dfindices['key'] == 'SECTORAL INDICES']

        dfindices = dfindices.filter(
            ['index', 'indexSymbol', 'last', 'open', 'high', 'low', 'previousClose', 'variation', 'percentChange',
             'declines', 'advances',
             'perChange365d', 'perChange30d','date365dAgo','date30dAgo','perChange30d','previousDay','oneWeekAgo','oneMonthAgo','oneYearAgo'])
#picking timestamp directly from request data
        dfindices['currentDate']=r['timestamp']
    except Exception as error:
        print("error {0}".format(error))
    return dfindices
##############################################################################################################
def collection_daily_insert(df,entity):
    try:
        if entity =='sectors' :
            niftydb.nsesectors.create_index([('index', pymongo.DESCENDING),('currentDate', pymongo.DESCENDING)], unique=True)
            niftydb.nsesectors.insert_many(df.to_dict('records'))
    except BulkWriteError as bwe:
            return print("sectors record already exists")
            # you can also take this component and do more analysis
            # werrors = bwe.details['writeErrors']
    try:
        if entity == 'stocks':
            niftydb.nsestocks.create_index([('symbol', pymongo.DESCENDING), ('lastUpdateTime', pymongo.DESCENDING),('Index', pymongo.DESCENDING)], unique=True)
            niftydb.nsestocks.insert_many(df.to_dict('records'))
    except BulkWriteError as bwe:
           return print("stocks record already exists")
                # you can also take this component and do more analysis
                # werrors = bwe.details['writeErrors']
    try:
        if entity == 'indiceshistory':
            niftydb.nse_indices_historical_data.create_index([('Index Name', pymongo.DESCENDING), ('Index Date', pymongo.DESCENDING)], unique=True)
            niftydb.nse_indices_historical_data.insert_many(df.to_dict('records'))
    except BulkWriteError as bwe:
            return print("Indices history record already exists")
    # you can also take this component and do more analysis
    # werrors = bwe.details['writeErrors']


##############################################################################################################

def indices_historical_data():
# This program is to fetch indices historical data for a given date range, date range defined above startdate and enddate
# as global parameters,this method also skips holidays
#
    df = pd.DataFrame()

    for single_date in daterange(startdate, enddate):
        try:
#Skipping NSE holidays, if current date is not in weekdays
            if single_date.weekday() not in weekdays:
                datestring = single_date.strftime("%d%m%Y")
                lastudpatedate = single_date.strftime("%d-%m-%Y")
                if datestring in holidays():
                    continue
#Fetching the data from NSE, data returned in csv format ##############################################
                url = "https://www1.nseindia.com/content/indices/ind_close_all_" + datestring + ".csv"
                r = requests.get(url=url).content
                df_sectors = pd.read_csv(io.StringIO(r.decode('utf8')), skiprows=0)
                df_sectors['lastudpatedate']=lastudpatedate

#Insert the records into mongo DB collection by calling generic method collection daily insert###############
                collection_daily_insert(df_sectors,'indiceshistory')

        except Exception as error:
            print("error {0}".format(error))
            continue
    return df
###################################Method indices_historical_data Ends here#####################################################
def read_sectorswisestocks():
    #This Method reads sector list from mongo nifty db ->nsesectors collection distinctly and the associate the index with the
    #stock data while inseting into Nifty DB stocks collection by passing sector list in stocks daily insert method
    index = niftydb.nsesectors.distinct("index")
    try:
        for sector in index:
            stocks_daily_insert(sector)
    except Exception as ex:
            print("Duplicate stocks data")
##############################################################################################################
def stocks_daily_insert(index):
    if index == "NIFTY OIL & GAS" :
        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20OIL%20%26%20GAS"
    else:
        url = "https://www.nseindia.com/api/equity-stockIndices?index=" + index
    headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/80.0.3987.100 Safari/537.36',
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/market-data/live-equity-market?symbol=NIFTY%20METAL"}

    try:
        # cookie_dict = get_session_cookies()
        cookie_dict = json.loads(open('../StockSelection/cookies').read())
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
    df = pd.DataFrame(r['data'])
    df = df.filter(['symbol', 'open', 'dayHigh', 'dayLow', 'lastPrice', 'previousClose','change','ffmc','yearHigh','yearLow', 'pChange', 'perChange365d',
                    'totalTradedVolume','totalTradedValue','lastUpdateTime','nearWKH','nearWKL'
                    'perChange30d'])
    df['Index'] = index
 #calling generic method to insert stocks data
    collection_daily_insert(df, 'stocks')

    return df

def main():
    df=pd.DataFrame()

    read_sectorswisestocks()
    df=sectors_daily_insert()
    collection_daily_insert(df,'sectors')
    indices_historical_data()
##############################################################################################################
if __name__ == '__main__':
    main()







