from datetime import datetime

import numpy as np
import xlwings as xw
import configparser
from utility import *
# Declaration of files and display setting for panda

config = configparser.RawConfigParser()
config.read('ConfigFile.properties')

path =config.get('filedetails', 'filelocation')
filename = config.get('filedetails','NseStockOptionChainHistory.filename')
excel_file = path+filename


# Declaration of files and display setting for panda.
from xlwings.constants import DeleteShiftDirection


symbol = 'HDFCBANK'

oi_filename = os.path.join(path+"/stockdata",
                           "OI"+symbol+"_data_{0}".format(datetime.now().strftime("%m%y")))
vol_filename = os.path.join(path+"/stockdata",
                            "VOL"+symbol+"_data_{0}".format(datetime.now().strftime("%m%y")))
wb = xw.Book(excel_file)

df_oi = pd.DataFrame()
df_vol = pd.DataFrame()
df1 = pd.DataFrame()

sheet_optionchain = wb.sheets("stockoption")
#sheet_optionchain = wb.sheets("OptionChainEOM")

intrtype = 'OPTSTK'


million =1000000
dt = datetime.today()
sdt =datetime.today()-timedelta(days=7)
expiry = (LastThInMonth(dt.year, dt.month)).strftime('%d-%b-%Y')
startdate = (date(sdt.year, sdt.month, sdt.day)).strftime('%d-%m-%Y')
# Todays date
enddate = (date(dt.year, dt.month, dt.day)).strftime('%d-%m-%Y')




#frm = date(2020, 3, 27)
#to =date(2020, 4, 18)

#Span is to find the largest OI data of call and puts
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



def optionchain(df_values,opttype):


    headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/80.0.3987.100 Safari/537.36',
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate"}

    url = ('https://www.nseindia.com/api/historical/fo/derivatives?&from='+startdate+'&to='+enddate+'&expiryDate='+expiry+'&optionType='+opttype+'&instrumentType='+intrtype+'&symbol='+symbol)
    print(url)

    # df = nse.get_history(symbol=symbol, start=frm, end=to)
    #
    # df.reset_index(inplace=True)
    #
    #
    #
    # df['Date'] = pd.to_datetime(df['Date'])
    # df['Date']=df.Date.dt.strftime('%d-%b-%Y')
    #
    r = requests.get(url, headers=headers).json()
   # r = requests.get(url, headers=headers).content
    print('printing r',r)
    oc_values = pd.DataFrame(r['data'])
    print('printing oc_values', oc_values)

 #   oc_values = pd.read_csv(io.StringIO(r.decode('utf8')))

   # oc_values.columns = oc_values.columns.str.replace(' ', '')

    print(oc_values)

    # fromt = datetime(2020,4,9,15,30)
    # tot = datetime(2020,4,30,15,30)
    # lt=tot-fromt
    # a =lt/timedelta(days=1)
    # print(a)
    # t= a/365
    # print(t)
    #
    # iv =implied_volatility(29.50,420.00,440.00,t,0.1,'c')
    # print('IV',iv)
    # quit()

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

    df_values['OI%change'] = df_values['OPENINTEREST'] - df_values['CHANGEINOI']

    df_values['OI%change'] = round((df_values['CHANGEINOI'] / df_values['OI%change']) * 100, 2)

    df_values['OPENPRICE'] = pd.to_numeric(df_values['OPENPRICE'], errors='ignore', downcast='float')

    # df_values['HIGHPRICE']=pd.to_numeric(df_values['HIGHPRICE'],errors='ignore', downcast='float')
    # df_values['LOWPRICE']=pd.to_numeric(df_values['LOWPRICE'], errors='ignore', downcast='float')
    #
    # df_values['openhigh'] = round((df_values['HIGHPRICE'] - df_values['OPENPRICE']), 2)
    # df_values['openlow'] = round((df_values['OPENPRICE'] - df_values['LOWPRICE']), 2)

    df_values['SETTLEPRICE'] = df_values['SETTLEPRICE'].astype(float)
    df_values['SettlePriceDiff_' + opttype] = round((df_values['SETTLEPRICE']-df_values['OPENPRICE'] ), 2)

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

    df_values['CHANGEINOI/Volume'] = df_values['OPENINTEREST'] / df_values['Volume']
    return df_values


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def main():


    df_values = pd.DataFrame()

    ce_values = pd.DataFrame()
    pe_values = pd.DataFrame()

    ce_values = optionchain(df_values, opttype='CE')
    pe_values = optionchain(df_values, opttype='PE')
    print(ce_values)
    print(pe_values)

    sheet_optionchain.range('3:50').clear_contents()

    sheet_optionchain.range("S1").options(index=False, headers=False).value = symbol
    sheet_optionchain.range("A2").options(index=False, headers=False).value = ce_values[
        ['EXPIRYDATE', 'OPTIONTYPE', 'OPENPRICE', 'HIGHPRICE', 'LOWPRICE', 'CLOSEPRICE',
         'LASTPRICE', 'SETTLEPRICE', 'Volume', 'VALUE', 'PREMIUMVALUE', 'OPENINTEREST', 'CHANGEINOI',
         'CHANGEINOI/Volume', 'STRIKEPRICE', 'DATE',
         'SettlePriceDiff_CE', 'OI%change', 'Direction_CE']]

    sheet_optionchain.range("T2").options(index=False, headers=False).value = pe_values[
        ['OPTIONTYPE', 'DATE', 'Direction_PE', 'OI%change', 'SettlePriceDiff_PE', 'STRIKEPRICE', 'OPENINTEREST',
         'CHANGEINOI', 'OPENPRICE', 'HIGHPRICE', 'LOWPRICE', 'CLOSEPRICE',
         'LASTPRICE', 'SETTLEPRICE', 'Volume', 'VALUE', 'PREMIUMVALUE', 'CHANGEINOI/Volume']]


if __name__ == '__main__':
    main()
