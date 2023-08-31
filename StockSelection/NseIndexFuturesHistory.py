import pandas as pd
import xlwings as xw
from datetime import date, datetime, time, timedelta
import io, requests, os, json, numpy as np
import configparser
# Declaration of files and display setting for panda

config = configparser.RawConfigParser()
config.read('ConfigFile.properties')

path =config.get('filedetails', 'filelocation')
filename = config.get('filedetails', 'NseIndexFuturesHistory.filename')
excel_file = path+filename
# Declaration of files and display setting for panda

oi_filename = os.path.join(path+"/IndexData",
                           "OINSE_data_{0}".format(datetime.now().strftime("%m%y")))
vol_filename = os.path.join(path+"/IndexData",
                            "VOLNSE_data_{0}".format(datetime.now().strftime("%m%y")))
wb = xw.Book(excel_file)

df_oi = pd.DataFrame()
df_vol = pd.DataFrame()
df1 = pd.DataFrame()

sheet_optionchain = wb.sheets("FuturesIndex")

symbol = 'NIFTY'
intrtype = 'FUTIDX'

expiry = "28-MAY-2020"
fromdate="27-04-2020"
fromdatefilter = "27-Apr-2020"
todate='28-05-2020'

span = 5

pd.set_option('display.width', 1500)
pd.set_option('display.max_columns', 75)
pd.set_option('display.max_rows', 10000)


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


df_values = pd.DataFrame()
# Index(['Unnamed: 0', 'Unnamed: 1', 'No. of Contracts', 'Amount in Crore', 'No. of Contracts.1', 'Amount in Crore.1',
# 'No. of Contracts.2', 'Amount in Crore.2'], dtype='object')


def optionchain(df_values):
    headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/80.0.3987.100 Safari/537.36',
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate"}

    url = ('https://www.nseindia.com/api/historical/fo/derivatives?&from='+fromdate+'&to='+todate+'&instrumentType=' + intrtype +'&symbol=' + symbol + '&csv=true')
    r = requests.get(url, headers=headers).content
    oc_values = pd.read_csv(io.StringIO(r.decode('utf8')))
    oc_values.columns = oc_values.columns.str.replace(' ', '')
    for data in oc_values['DATE']:
        try:
            df_values1 = oc_values[oc_values['DATE'] == data]
            df_values = pd.concat([df_values, df_values1])

        except Exception as error:
            print("error {0}".format(error))
            continue
    df_values.drop_duplicates(subset='OPENINTEREST', keep='first', inplace=True)
    df_values['EXPIRYDATE']=pd.to_datetime(df_values['EXPIRYDATE'])
    df_values['DATE']=pd.to_datetime(df_values['DATE'])

    df_values.sort_values(['EXPIRYDATE','DATE'], ascending=[True,False], inplace=True)
    df_values.replace({',': ''}, regex=True, inplace=True)

    df_values['OI%change']=df_values['OPENINTEREST']-df_values['CHANGEINOI']

    df_values['OI%change'] = round((df_values['CHANGEINOI']/df_values['OI%change'] )*100,2)

    df_values['SETTLEPRICE'] = df_values['SETTLEPRICE'].astype(float)
    df_values['SettlePriceDiff'] = df_values['SETTLEPRICE'].diff(periods=-1)
    df_values['Longs'] = np.where(
        ((df_values['SettlePriceDiff'] > 0) & (df_values['CHANGEINOI'] > 0)), 'True', 'False')

    df_values['Unwinding'] = np.where(
        ((df_values['SettlePriceDiff'] < 0) & (df_values['CHANGEINOI'] < 0)), 'True', 'False')
    df_values['Shorts'] = np.where(
        ((df_values['SettlePriceDiff'] < 0) & (df_values['CHANGEINOI'] > 0)), 'True', 'False')
    df_values['ShortsCovering'] = np.where(
        ((df_values['SettlePriceDiff'] > 0) & (df_values['CHANGEINOI'] < 0)), 'True', 'False')

    df_values['Direction'] = np.where((df_values.Longs == 'True'), 'Longs',
                                         np.where((df_values.ShortsCovering == 'True'), 'ShortsCovering',
                                                  np.where((df_values.Unwinding == 'True'), 'Unwinding',
                                                           np.where((df_values.Shorts == 'True'), 'Shorts',
                                                                    'Nothing'))))

    df_values['OI/Volume'] = round((df_values['OPENINTEREST'] / df_values['Volume']),2)
    df_values['OPENINTEREST'] = df_values.OPENINTEREST.astype(float) / 75
    df_values['CHANGEINOI'] = round((df_values.CHANGEINOI.astype(float) / 75),2)

    df_values['PREMIUMVALUE'] = round((df_values.PREMIUMVALUE.astype(float) / 1000000),2)
    df_values['VALUE'] = round((df_values.VALUE.astype(float) / 1000000),2)
    df_values['Volume'] = round((df_values.Volume.astype(float) / 1000000),2)


    return df_values

def main():
    # nsdl_data()
    df_values = pd.DataFrame()
    ce_values =pd.DataFrame()
    ce_values= optionchain(df_values)
    print(ce_values)
    ce_values=ce_values[::-1]
    sheet_optionchain.range("A2").options(index=False, headers=False).value = ce_values[
    ['EXPIRYDATE', 'OPTIONTYPE', 'OPENPRICE', 'HIGHPRICE', 'LOWPRICE', 'CLOSEPRICE',
    'LASTPRICE', 'SETTLEPRICE', 'Volume', 'VALUE', 'PREMIUMVALUE', 'OPENINTEREST', 'CHANGEINOI', 'OI/Volume', 'STRIKEPRICE', 'DATE',
    'SettlePriceDiff', 'OI%change', 'Direction']]


if __name__ == '__main__':
    main()
