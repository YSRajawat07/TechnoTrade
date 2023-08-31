import configparser
import warnings
import xlwings as xw
from datetime import datetime, timedelta, date
import numpy as np
from StockSelection.NseDataFetch import funstockprice
from StockSelection.utility import *
import pymongo

#get_session_cookies()
# Declaration of files and display setting for panda

warnings.simplefilter(action="ignore")

config = configparser.RawConfigParser()
config.read('ConfigFile.properties')


dt = date.today()


sdt = date.today()-timedelta(days=90)
stkstartdate = date(sdt.year, sdt.month, sdt.day)

# Todays date
stkenddate = date(dt.year, dt.month, dt.day)
startdate = str(sdt.day)+"-"+str(sdt.month)+"-"+str(sdt.year)
enddate = str(dt.day-1)+"-"+str(dt.month)+"-"+str(dt.year)


month = dt.month
thresholdamt=9000000
million = 100000
pd.set_option('display.width', 1500)
pd.set_option('display.max_columns', 75)
pd.set_option('display.max_rows', 10000)

#MongoDB connections
client = pymongo.MongoClient("mongodb://localhost:27017/")
niftydb = client["NiftyDB"]
print('DB connection established')

def positions():

    url = "https://www.nseindia.com/api/corporates-pit?index=equities&from_date="+startdate+"&to_date="+enddate+"&json=true"
    #url = "https://www.nseindia.com/api/corporates-pit?index=equities&from_date=04-04-2020&to_date=13-07-2020&json=true"

    url2="https://www.nseindia.com/api/corporate-sast-reg29?"
    url3="https://www.nseindia.com/api/corporate-pledgedata?index=equities&from_date="+startdate+"&to_date="+enddate+""

    headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/80.0.3987.100 Safari/537.36',
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www1.nseindia.com/live_market/dynaContent/live_watch/equities_stock_watch.htm"}
    try:
        # cookie_dict = get_session_cookies()
        print('before json.loads ')
        cookie_dict = json.loads(open('cookies').read())

        print('after  json.loads ')
    except Exception as error:
        print("Error reading cookies most active2")
        cookie_dict = get_session_cookies()
    print('before session request')
    session = requests.session()
    print('after session request')

    for cookie in cookie_dict:
        if "bm_sv" in cookie or 'nseappid' in cookie or "nsit" in cookie:
            session.cookies.set(cookie, cookie_dict[cookie])
    try:
        r = session.get(url, headers=headers, verify=False).json()
        print(url)

    except Exception as error:
        print('error in reading cookies')
        for cookie in cookie_dict:
            if "bm_sv" in cookie or 'nseappid' in cookie or "nsit" in cookie:
                session.cookies.set(cookie, cookie_dict[cookie])
        r = session.get(url, headers=headers, verify=False).json()

    try:
        r2 = session.get(url2, headers=headers, verify=False).json()
    except Exception as error:
        print('error in reading cookies')
        for cookie in cookie_dict:
            if "bm_sv" in cookie or 'nseappid' in cookie or "nsit" in cookie:
                session.cookies.set(cookie, cookie_dict[cookie])
        r2 = session.get(url2, headers=headers, verify=False).json()

    try:
        r3 = session.get(url3, headers=headers, verify=False).json()
    except Exception as error:
        print('error in reading cookies')
        for cookie in cookie_dict:
            if "bm_sv" in cookie or 'nseappid' in cookie or "nsit" in cookie:
                session.cookies.set(cookie, cookie_dict[cookie])
        r3 = session.get(url3, headers=headers, verify=False).json()


    promotersgroup = pd.DataFrame(r['data'])
    sharessell = pd.DataFrame(r2['data'])
    sharesholdingandpledged = pd.DataFrame(r3['data'])

 #   print(promotersgroup.to_dict('records'))

    recpg= promotersgroup[promotersgroup['personCategory'] =='Promoter Group']
    recp= promotersgroup[promotersgroup['personCategory'] =='Promoters']

    sharessellgp= sharessell[['symbol','time','noOfShareSale']]
    recselltime= sharessell[['symbol','time']]
    sharessellgp.replace(to_replace=[None], value=0, inplace=True)
    sharessellgp['noOfShareSale'] = round(sharessellgp['noOfShareSale'].astype(int), 2)
    recsharessell= round(sharessellgp.groupby(['symbol'], sort=False)["noOfShareSale"].sum().reset_index(name='noOfShareSale'), 2)
    recsharessell = pd.merge(recselltime, recsharessell, how='inner', on='symbol')
    recsharessell=recsharessell[recsharessell.groupby('symbol').time.transform('max') == recsharessell['time']] \
        .drop_duplicates(keep='first').reset_index(drop=True)

    df = pd.concat([recpg, recp])
    dfsymbol=df[['symbol','company']].drop_duplicates(keep='first').reset_index(drop=True)

    sharesholdingandpledged = sharesholdingandpledged[['comName', 'percPromoterHolding', 'percSharesPledged','sharesCollateral','broadcastDt']]
    sharesholdingandpledged = sharesholdingandpledged.rename(columns={'comName': 'company'})
    sharesholdingandpledged = pd.merge(sharesholdingandpledged, dfsymbol, how='inner', on='company')
    sharesholdingandpledged = sharesholdingandpledged[['symbol', 'percPromoterHolding', 'percSharesPledged','sharesCollateral','broadcastDt']]

    rec=df.filter(['symbol','secVal','secAcq','date'])


    rec['date']=    pd.to_datetime(rec['date']).dt.strftime('%d-%m-%Y')

    symbollist = rec['symbol'].drop_duplicates( keep="first")


    dfBuyFilter = df[df['acqMode'] == 'Market Purchase'].filter(['symbol', 'secVal', 'secAcq', 'date'])

    dfSellFilter = df[df['acqMode'] == 'Market Sale'].filter(['symbol', 'secVal', 'secAcq','date'])
    dfBuyFilter['secVal'] = round(dfBuyFilter['secVal'].astype(float),2)
    dfBuyFilter['secAcq'] = round(dfBuyFilter['secAcq'].astype(float),2)
    dfBuyFilter['date'] = dfBuyFilter['date']
    dfSellFilter['secVal'] = round(dfSellFilter['secVal'].astype(float),2)
    dfSellFilter['secAcq'] = round(dfSellFilter['secAcq'].astype(float),2)

    ###################Promoters Buy calculations######################################


    dfBuyAmount = round(dfBuyFilter.groupby(['symbol'], sort=False)["secVal"].sum().reset_index(name='TotalBuyAmount'),2)
    dfBuySymbolDate = dfBuyFilter[['symbol','date']]

    dfBuyDate = dfBuySymbolDate[dfBuySymbolDate.groupby('symbol').date.transform('max') ==dfBuySymbolDate['date']]\
        .drop_duplicates(keep='first').reset_index(drop=True)

    dfBuyShares = round(dfBuyFilter.groupby(['symbol'], sort=False)["secAcq"].sum().reset_index(name='TotalBuyShares'),2)
    dfBuy = pd.merge(dfBuyAmount,dfBuyDate,how='inner', on='symbol')
    dfBuy = pd.merge(dfBuy, dfBuyShares, how='inner', on='symbol')
    dfBuy =round(dfBuy[dfBuy.TotalBuyAmount > thresholdamt],2)
    dfBuy['avgBuyPrice'] = round(dfBuy.TotalBuyAmount.astype(int)/dfBuy.TotalBuyShares.astype(int),2)

    ###################Promoters Buy  calculations ENDS######################################

    ###################Promoters sell calculations######################################
    dfSellSymbolDate = dfSellFilter[['symbol', 'date']]
    dfSellDate = dfSellSymbolDate[dfSellSymbolDate.groupby('symbol').date.transform('max') ==dfSellSymbolDate['date']]\
        .drop_duplicates(keep='first').reset_index(drop=True)
    dfSellAmount = round(dfSellFilter.groupby(['symbol'], sort=False)["secVal"].sum().reset_index(name='TotalSellAmount'),2)
    dfSellShares = round(dfSellFilter.groupby(['symbol'], sort=False)["secAcq"].sum().reset_index(name='TotalSellShares'),2)
    dfSell = pd.merge(dfSellAmount, dfSellDate, how='inner', on='symbol')
    dfSell = pd.merge(dfSell, dfSellShares, how='inner', on='symbol')
    dfSell =round(dfSell[dfSell.TotalSellAmount > thresholdamt],2)
    dfSell['avgSellPrice'] = round(dfSell.TotalSellAmount.astype(int) / dfSell.TotalSellShares.astype(int), 2)

    dfBuy.sort_values(by=['date'], inplace=True)
#    dfSell.sort_values(by=['date'], ascending=False,inplace=True)
    dfCombi =dfBuy #pd.merge(dfBuy,dfSell,how='outer', on='symbol')
    dfCombi = pd.merge(dfCombi,recsharessell,how='outer', on='symbol')
    dfCombi = pd.merge(dfCombi,sharesholdingandpledged,how='outer', on='symbol')
    print('before calling funstockprice')
    stockprice= funstockprice()


    dfCombi =dfCombi.fillna(0)
    dfCombi = dfCombi.applymap(lambda x: int(round(x, 2)) if isinstance(x, (int, float)) else x)

  #  dfCombi = np.where((dfCombi['percPromoterHolding'].apply(lambda x: float(x)) >=48))


 #   dfCombi = np.where((dfCombi['percSharesPledged'].apply(lambda x: float(x)) <=0.5))

    dfCombi=pd.merge(dfCombi, stockprice, how='inner', on='symbol')
    print(dfCombi.head(100))
    niftydb.nse_promotersbuying_stocks.create_index([('symbol', pymongo.DESCENDING), ('date', pymongo.DESCENDING)],
                                                    unique=True)
    niftydb.nse_promotersbuying_stocks.insert_many(dfCombi.to_dict('records'))



def main():

    # if (startref <= datetime.now().time() <= endref):
    #     print("Please run this after 7:00 PM to get the values, closing the program")
    #     quit()

    datestring = datetime.today().strftime("%d%m%Y")
    if datestring in holidays():
        print('Its NSE holiday so no files to print,hence quitting')
        quit()
   # get_session_cookies()
    positions()

if __name__ == '__main__':
    main()