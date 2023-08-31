import pandas as pd
import numpy as np
from time import sleep
import xlwings as xw
import io, requests, os, json
from utility import niftyexpiry, LastThInMonth, get_session_cookies
import urllib3
from datetime import datetime, timedelta
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



symbol = 'NIFTY'
spotsymbol = 'NIFTY 50'
vixymbol = 'INDIA VIX'


dt = datetime.today()
dtpreviousday = datetime.today() - timedelta(days=1)

expiry = niftyexpiry()
print(expiry)

expiry = expiry[0]
print('printing expiry ')
print(expiry)

lth= LastThInMonth(dt.year,dt.month)

expiry_fut =lth.strftime('%d-%b-%Y')

datestring = dtpreviousday.strftime("%d%m%Y")
print('printing expiry future')
print(lth.strftime('%d-%b-%Y'))


indexval = 0

df_list = []
mp_list = []

mpdict_activecall = []
mpdict_activeput = []

print(expiry)

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)




def getlivefuture(mpfut_df):
    headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/80.0.3987.100 Safari/537.36',
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Referer": "https://www.nseindia.com/market-data/equity-derivatives-watch"}

    #url = "https://www.nseindia.com/api/liveEquity-derivatives?index=nse50_fut"
    url = "https://www.nseindia.com/api/liveEquity-derivatives?index=nse50_fut"

    print('Reading futures..')

    try:
        # cookie_dict = get_session_cookies()
        cookie_dict = json.loads(open('cookies').read())
    except Exception as error:
        print("Error reading cookies most getlivefuture")
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

    print('Reading futures request captured..')
   # r  = json.load(urllib.request.urlopen(url))
    ce_values = r['data']

    ce_values = pd.DataFrame(ce_values)

    ce_values = ce_values[ce_values.expiryDate == expiry_fut]

    ce_values = ce_values.filter(
        ['underlying', 'lastPrice', 'change', 'pChange', 'volume', 'totalTurnover', 'value', 'premiumTurnOver',
         'underlyingValue', 'openInterest', 'noOfTrades'])

    print('printing future ce values.')

    print(ce_values)
    try:
        mp_dict_fut = {datetime.now().strftime("%d-%m %H:%M"): {'underlying': ce_values.underlyingValue[indexval],
                                                                'lastPrice': ce_values.lastPrice[indexval],
                                                                'change': ce_values.change[indexval],
                                                                'pChange': ce_values.pChange[indexval],
                                                                'volume': ce_values.volume[indexval],
                                                                'totalTurnover': ce_values.totalTurnover[indexval],
                                                                'value': ce_values.value[indexval],
                                                                'premiumTurnOver': ce_values.premiumTurnOver[indexval],
                                                                'openInterest': ce_values.openInterest[indexval],
                                                                'noOfTrades': ce_values.noOfTrades[indexval],
                                                                }}

        print('printing future dictionary.',mp_dict_fut)

        dffut = pd.DataFrame(mp_dict_fut).transpose()

        mpfut_df = pd.concat([mpfut_df, dffut], sort=False)

        print('Future data successfully read')
    except Exception as error:
        print("Error in Futures . Error : {0}".format(error))


    return mpfut_df


def niftyspotfun():
    headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/80.0.3987.100 Safari/537.36',
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        # "Referer": "https://www1.nseindia.com/live_market/dynaContent/live_watch/get_quote/GetQuoteFO.jsp?underlying=NIFTY&instrument=FUTIDX&expiry=7MAY2020&type=-&strike=-"}
        "Referer": "https://www.nseindia.com/market-data/live-market-indices"}

    url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
    #url = "https://www.nseindia.com/api/allIndices"
    try:
        # cookie_dict = get_session_cookies()
        cookie_dict = json.loads(open('cookies').read())
    except Exception as error:
        print("Error reading cookies most niftyspotfun")
        cookie_dict = get_session_cookies()

    session = requests.session()

    for cookie in cookie_dict:
        if "bm_sv" in cookie or 'nseappid' in cookie or "nsit" in cookie:
            session.cookies.set(cookie, cookie_dict[cookie])
    try:
        r = session.get(url, headers=headers, verify=False).json()
    except Exception as error:
        print('error in reading cookies niftyspotfun')
        for cookie in cookie_dict:
            if "bm_sv" in cookie or 'nseappid' in cookie or "nsit" in cookie:
                session.cookies.set(cookie, cookie_dict[cookie])
        r = session.get(url, headers=headers, verify=False).json()
    rec = pd.DataFrame(r['data'])


    rec = rec.filter(
        ['symbol', 'open', 'dayHigh', 'dayLow', 'lastPrice', 'previousClose', 'change', 'pChange', 'totalTradedVolume',
         'totalTradedValue', 'lastUpdateTime'])
    nifty = rec[rec.symbol == spotsymbol]

    return nifty


def mostactive():
    headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/80.0.3987.100 Safari/537.36',
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Referer": "https://www.nseindia.com/get-quotes/derivatives?symbol=NIFTY"}

    url = "https://www.nseindia.com/api/equity-stock?index=opt_nifty50"
    #url = "https://www.nseindia.com/api/quote-derivative?symbol=NIFTY"
    try:
        # cookie_dict = get_session_cookies()
        cookie_dict = json.loads(open('cookies').read())
    except Exception as error:
        print("Error reading cookies most mostactive")
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
    rec = pd.DataFrame(r['value'])
    reccall = rec[rec['optionType'] == 'Call'].head(3)
    recput = rec[rec['optionType'] == 'Put'].head(3)

    reccall.reset_index(inplace=True, drop=True)
    recput.reset_index(inplace=True, drop=True)

    reccall['value'] = round(reccall.totalTurnover, 2)
    recput['value'] = round(recput.totalTurnover, 2)

    reccall['premiumTurnOver'] = round((reccall.premiumTurnover / 1000000000), 2)
    recput['premiumTurnOver'] = round((recput.premiumTurnover / 1000000000), 2)

    reccall['NoOfTrades'] = round((reccall.numberOfContractsTraded / 100000), 2)
    recput['NoOfTrades'] = round((recput.numberOfContractsTraded / 100000), 2)

    reccall = reccall.filter(
        ['strikePrice', 'lastPrice', 'change', 'pChange', 'volume',
         'value', 'premiumTurnOver', 'underlyingValue', 'openInterest', 'NoOfTrades'])

    recput = recput.filter(
        ['strikePrice', 'lastPrice', 'change', 'pChange', 'volume',
         'value', 'premiumTurnOver', 'underlyingValue', 'openInterest', 'NoOfTrades'])

    return reccall, recput


def indiavix():
    headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/80.0.3987.100 Safari/537.36',
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Referer": "https://www1.nseindia.com/live_market/dynaContent/live_watch/get_quote/GetQuoteFO.jsp?underlying=NIFTY&instrument=FUTIDX&expiry=7MAY2020&type=-&strike=-"}

    url = "https://www.nseindia.com/api/allIndices"
    try:
        # cookie_dict = get_session_cookies()
        cookie_dict = json.loads(open('cookies').read())
    except Exception as error:
        print("Error reading cookies most active2")
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
    # ce_values = [data['CE'] for data in r['records']['data'] if
    #              "CE" in data and str(data['expiryDate']).lower() == str(expiry).lower()]
    rec = pd.DataFrame(r['data'])

    rec = rec.filter(
        ['key', 'indexSymbol', 'last', 'variation', 'percentChange', 'open', 'high', 'low', 'previousClose',
         'declines', 'advances', 'unchanged'])

    vix = rec[rec.indexSymbol == vixymbol]
    return vix


def niftyoptionchain():
    global df
    url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
    headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
        "Accept-Language": "en-US,en;q=0.9", "Accept-Encoding": "gzip, deflate"}

    try:
        # cookie_dict = get_session_cookies()
        cookie_dict = json.loads(open('cookies').read())
    except Exception as error:
        print("Error reading cookies most niftyoptionchain")
        cookie_dict = get_session_cookies()

    session = requests.session()

    for cookie in cookie_dict:
        if "bm_sv" in cookie or 'nseappid' in cookie or "nsit" in cookie:
            session.cookies.set(cookie, cookie_dict[cookie])


    tries = 1
    max_retries = 3
    while tries <= max_retries:
        try:
            try:
                r = session.get(url, headers=headers, verify=False).json()
            except Exception as error:
                print('error in reading cookies')
                for cookie in cookie_dict:
                    if "bm_sv" in cookie or 'nseappid' in cookie or "nsit" in cookie:
                        session.cookies.set(cookie, cookie_dict[cookie])
                        r = session.get(url, headers=headers, verify=False).json()

            if expiry:
                ce_values = [data['CE'] for data in r['records']['data'] if
                             "CE" in data and str(data['expiryDate']).lower() == str(expiry).lower()]

                pe_values = [data['PE'] for data in r['records']['data'] if
                             "PE" in data and str(data['expiryDate']).lower() == str(expiry).lower()]
                ce_data = pd.DataFrame(ce_values)
                pe_data = pd.DataFrame(pe_values)

                ce_data['type'] = "CE"
                pe_data['type'] = "PE"
                #df = pd.concat([ce_data, pe_data])
                df = ce_data.join(pe_data, how='left', lsuffix='_Call', rsuffix='_Put')
                return df,ce_data,pe_data

        except Exception as error:
            print("error {0} function niftyoptionchain".format(error))
            tries += 1
            sleep(10)
            continue
        if tries >= max_retries:
            print("Max retries exceeded. No new data time {0}".format(datetime.now()))
            return df


def funstockprice():
    print('inside function funstockprice')
    url ="https://archives.nseindia.com/archives/nsccl/volt/CMVOLT_"+datestring+".CSV"


    try:
        # cookie_dict = get_session_cookies()
        cookie_dict = json.loads(open('cookies').read())
    except Exception as error:
        print("Error reading cookies most niftyoptionchain")
        cookie_dict = get_session_cookies()

    session = requests.session()

    for cookie in cookie_dict:
        if "bm_sv" in cookie or 'nseappid' in cookie or "nsit" in cookie:
            session.cookies.set(cookie, cookie_dict[cookie])
    print('inside funstockprice get URL content')
    print(url)
    rvol = requests.get(url).content
    print('printing rvol')
    dfstockdetails = pd.read_csv(io.StringIO(rvol.decode('utf8')))
    dfstockdetails=dfstockdetails[['Date', 'Symbol', 'Underlying Close Price (A)']]
    dfstockdetails=dfstockdetails.rename(columns={'Underlying Close Price (A)': 'Price', 'Symbol': 'symbol'})

    return dfstockdetails


def main():
    funstockprice()


if __name__ == '__main__':
    main()

