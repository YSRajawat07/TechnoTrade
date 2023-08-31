import pandas as pd
import xlwings as xw
from datetime import date, datetime, time, timedelta
import io, requests, os, json,numpy as np
import os
import configparser
# Declaration of files and display setting for panda
from utility import niftyexpiry

config = configparser.RawConfigParser()
config.read('ConfigFile.properties')

path =config.get('filedetails', 'filelocation')
filename = config.get('filedetails','NseIndexOptionchainHistory.filename')
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



sheet_optionchain = wb.sheets("OptionChain")
#sheet_optionchain = wb.sheets("OptionChainEOM")
symbol = 'NIFTY'
intrtype = 'OPTIDX'

expiry = niftyexpiry()

expiry = expiry[0]

dt = datetime.today()
startdate = (date(dt.year, dt.month, dt.day-7)).strftime('%d-%m-%Y')
# Todays date
enddate = (date(dt.year, dt.month, dt.day)).strftime('%d-%m-%Y')


fromdate="10-07-2020"
#fromdatefilter = "27-Mar-2020"
todate='20-07-2020'

span=5

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

def underlyinghistory():

    headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/80.0.3987.100 Safari/537.36',
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate"}

    url_idx = "https://www1.nseindia.com/products/dynaContent/equities/indices/historicalindices.jsp?indexType=NIFTY%2050&fromDate="+startdate+"&toDate="+enddate
    print(url_idx)
    r_idx = requests.get(url_idx, headers=headers).text
    idx_data = pd.read_html(r_idx,header=None)

    idx_data=pd.DataFrame(idx_data[0])
    idx_data.columns = idx_data.columns.get_level_values(2)
    idx_data.drop(idx_data.tail(1).index, inplace=True)

    idx_data.columns = idx_data.columns.str.replace(' ', '')
    print(idx_data)

    idx_data['SharesTraded'] = idx_data['SharesTraded'].str.replace('-', '0')
    idx_data['Turnover(Cr)'] = idx_data['Turnover(Cr)'].str.replace('-', '0')



    idx_data['avgvol']= idx_data['SharesTraded'].rolling(7).mean()


    sheet_optionchain.range("K100").options(index=False, headers=False).value = idx_data[::-1]

    return idx_data
           #idx_data.drop(idx_data.filter(regex='period').columns, inplace=True, axis=1)


def optionchain(df_values,opttype):

    headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/80.0.3987.100 Safari/537.36',
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate"}

    url = ('https://www.nseindia.com/api/historical/fo/derivatives?&from='+startdate+'&to='+enddate+'&expiryDate='+expiry+'&optionType='+opttype+'&instrumentType='+intrtype+'&symbol='+symbol+'&csv=true')
    print(url)

    r = requests.get(url, headers=headers).content


    oc_values = pd.read_csv(io.StringIO(r.decode('utf8')))
    oc_values.columns = oc_values.columns.str.replace(' ', '')


    for data in oc_values['DATE']:
        try:

            df_values1 = oc_values[oc_values['DATE'] == data].nlargest(span, 'OPENINTEREST', keep='last')
            df_values = pd.concat([df_values, df_values1])

        except Exception as error:
            print("error {0}".format(error))
            continue


    df_values.drop_duplicates(subset='OPENINTEREST', keep='first', inplace=True)
    df_values['DATE'] = pd.to_datetime(df_values['DATE'])
    df_values.sort_values(['STRIKEPRICE', 'DATE'], ascending=[False, False], inplace=True)
    df_values.replace({',': ''}, regex=True, inplace=True)
    df_values.replace({'-': 0}, regex=True, inplace=True)

    df_values['OI%change']=df_values['OPENINTEREST']-df_values['CHANGEINOI']

    df_values['OI%change'] = round((df_values['CHANGEINOI']/df_values['OI%change'] )*100,2)

    df_values['OPENPRICE']=pd.to_numeric(df_values['OPENPRICE'], errors='ignore', downcast='float')

    # df_values['HIGHPRICE']=pd.to_numeric(df_values['HIGHPRICE'],errors='ignore', downcast='float')
    # df_values['LOWPRICE']=pd.to_numeric(df_values['LOWPRICE'], errors='ignore', downcast='float')
    #
    # df_values['openhigh'] = round((df_values['HIGHPRICE'] - df_values['OPENPRICE']), 2)
    # df_values['openlow'] = round((df_values['OPENPRICE'] - df_values['LOWPRICE']), 2)



    df_values['SETTLEPRICE'] = df_values['SETTLEPRICE'].astype(float)
    df_values['SettlePriceDiff_' + opttype] = round((df_values['SETTLEPRICE']-df_values['OPENPRICE']), 2)


    df_values['Longs_' + opttype] = np.where(
        ((df_values['SettlePriceDiff_' + opttype] > 0) & (df_values['CHANGEINOI'] > 0)), 'True', 'False')

    df_values['Unwinding_' + opttype] = np.where(
        ((df_values['SettlePriceDiff_' + opttype] < 0) & (df_values['CHANGEINOI'] < 0)), 'True', 'False')
    df_values['Shorts_' + opttype] = np.where(
        ((df_values['SettlePriceDiff_' + opttype] < 0) & (df_values['CHANGEINOI'] > 0)), 'True', 'False')
    df_values['ShortsCovering_' + opttype] = np.where(
        ((df_values['SettlePriceDiff_' + opttype] > 0) & (df_values['CHANGEINOI'] < 0)), 'True', 'False')
    if (opttype == 'CE'):

        df_values['Direction_CE'] = np.where((df_values.Longs_CE == 'True'), 'Longs',
                                             np.where((df_values.ShortsCovering_CE == 'True'), 'ShortsCovering',
                                                      np.where((df_values.Unwinding_CE == 'True'), 'Unwinding',
                                                               np.where((df_values.Shorts_CE == 'True'), 'Shorts',
                                                                        'Nothing'))))

    else:
        df_values['Direction_PE'] = np.where((df_values.Longs_PE == 'True'), 'Longs',
                                             np.where((df_values.ShortsCovering_PE == 'True'), 'ShortsCovering',
                                                      np.where((df_values.Unwinding_PE == 'True'), 'Unwinding',
                                                               np.where((df_values.Shorts_PE == 'True'), 'Shorts',
                                                                        'Nothing'))))

    df_values['OI/Volume'] = df_values['OPENINTEREST'] / df_values['Volume']
    df_values['EXPIRYDATE']=expiry
    return df_values
def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)
def main():

    df_undrlyng =pd.DataFrame()
    sheet_optionchain.range('3:1000').clear_contents()

    df_undrlyng= underlyinghistory()

    df_values = pd.DataFrame()

    ce_values =pd.DataFrame()
    pe_values =pd.DataFrame()

    ce_values= optionchain(df_values,opttype='CE')
    pe_values= optionchain(df_values,opttype='PE')

    dividevalue=1000000

    ce_values['VALUE'] =round(ce_values['VALUE'].astype(float),2) /dividevalue
    ce_values['PREMIUMVALUE'] =round(ce_values['PREMIUMVALUE'].astype(float),2) /dividevalue
    pe_values['PREMIUMVALUE'] = round(pe_values['PREMIUMVALUE'].astype(float),2) / dividevalue
    pe_values['VALUE'] = round(pe_values['VALUE'].astype(float),2) / dividevalue

    print(ce_values)
    print(pe_values)


    sheet_optionchain.range("S1").options(index=False, headers=False).value = symbol
    sheet_optionchain.range("A2").options(index=False, headers=False).value = ce_values[
        ['EXPIRYDATE','OPTIONTYPE', 'OPENPRICE', 'HIGHPRICE', 'LOWPRICE', 'CLOSEPRICE',
    'LASTPRICE', 'SETTLEPRICE', 'Volume', 'VALUE', 'PREMIUMVALUE', 'OPENINTEREST', 'CHANGEINOI','OI/Volume','STRIKEPRICE','DATE',
         'SettlePriceDiff_CE','OI%change','Direction_CE']]

    sheet_optionchain.range("T2").options(index=False, headers=False).value = pe_values[
        ['OPTIONTYPE','DATE','Direction_PE','OI%change','SettlePriceDiff_PE', 'STRIKEPRICE', 'OPENINTEREST','CHANGEINOI','OPENPRICE', 'HIGHPRICE', 'LOWPRICE', 'CLOSEPRICE',
         'LASTPRICE', 'SETTLEPRICE', 'Volume', 'VALUE', 'PREMIUMVALUE','OI/Volume']]
if __name__ == '__main__':
    main()
