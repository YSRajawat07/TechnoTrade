import xlwings as xw
import numpy as np
from utility import *
import configparser
# Declaration of files and display setting for panda

config = configparser.RawConfigParser()
config.read('ConfigFile.properties')

path =config.get('filedetails', 'filelocation')
filename = config.get('filedetails','NseParticipantWiseOI&Vol.filename')
excel_file = path+filename

dt = impdt.datetime.today()
sdt =datetime.today()-timedelta(days=7)
pd.options.mode.chained_assignment = None

html_file= "/Files/fii.html"

oi_filename = os.path.join(path+"/IndexData", "OINSE_data_{0}".format(datetime.now().strftime("%m%y")))
vol_filename = os.path.join(path+"/IndexData", "VOLNSE_data_{0}".format(datetime.now().strftime("%m%y")))
#holidays=['02042020','06042020','10042020','14042020','01052020','25052020']


wb = xw.Book(excel_file)

df_listoi=[]
df_listvol=[]

weekdays = [5, 6]

clientType = ['FII','DII','Pro','Client']


df_oi = pd.DataFrame()
df_vol = pd.DataFrame()
df1 = pd.DataFrame()

sheet_FII = wb.sheets("FII")
sheet_DII = wb.sheets("DII")
sheet_Pro = wb.sheets("Pro")
sheet_Client = wb.sheets("Client")
sheet_fiidata = wb.sheets("FIIData")

startdate = date(sdt.year, sdt.month, sdt.day)
enddate = date(dt.year, dt.month, dt.day)

pd.set_option('display.width', 1500)
pd.set_option('display.max_columns', 75)
pd.set_option('display.max_rows', 10000)


def smartmoney(df,df_oi,df_vol,flag,dateindex):

    df.columns = df.columns.str.strip().str.lower().str.replace('\t ', '')

    df['NetFutures'+flag] = (df['future index long'] - df['future index short'])
    df['NetFuturesStocks'+flag] = (df['future stock long'] - df['future stock short'])

    df['IndexLong'+flag] = (df['option index call long'] + df['option index put short'])
    df['IndexShort'+flag] = (df['option index put long'] + df['option index call short'] )


    df['Index-conclusion'+flag] = np.where((df['IndexLong'+flag] >= df['IndexShort'+flag]), 'Bullish',
                                             np.where((df['IndexLong'+flag] < df['IndexShort'+flag]), 'Bearish','Nothing'))

    df['StockLong'+flag] = (df['option stock call long'] + df['option stock put short'])
    df['StockShort'+flag] = (df['option stock put long'] + df['option stock call short'] )

    df['Stock-conclusion'+flag] = np.where((df['StockLong'+flag] >= df['StockShort'+flag]), 'Bullish',
                                             np.where((df['StockLong'+flag] < df['StockShort'+flag]), 'Bearish',
                                                      'Nothing'))




    # df['Bullish'+flag] = (df['option index call long']+ df['option index put short'])
    # df['Bearish'+flag] = (df['option stock put long'] + df['option stock call short'])
    #
    # df['difference'+flag]= (df['Bullish']-df['Bearish'])

    # df_data = df[['client type'+flag, 'NetFutures'+flag, 'NetOptionIndexCall'+flag, 'NetOptionIndexPut'+flag, 'NetOptionStockCall'+flag,
    #               'NetOptionStockPut'+flag]]
    #
    #
    # df_data = df_data[:-1]
    # df_data['Date'] = datetime.now().strftime("%d-%m-%Y")
    # df_data.set_index('Date', inplace=True)
    # df_data['NetOption'+flag] = (df_data['NetOptionIndexCall'] - df_data['NetOptionIndexPut'])
    # df_data['NetCash'+flag] = (df_data['NetOptionStockCall'] - df_data['NetOptionStockPut'])
    #
    # df_data = df_data[['client type'+flag, 'NetOption'+flag, 'NetFutures'+flag, 'NetCash'+flag]]

    df['Date'] = dateindex

    df.set_index('Date', inplace=True)



    if flag =='oi':

        df_oi = pd.concat([df_oi, df])

        df_listoi.append(df.to_dict())

        with open(oi_filename, "w") as files:
            files.write(json.dumps(df_listoi, indent=4, sort_keys=True))
        return df_oi
    else:
        df_vol = pd.concat([df_vol, df])
        df_listvol.append(df.to_dict())
        with open(vol_filename, "w") as files:
            files.write(json.dumps(df_listvol, indent=4, sort_keys=True))
        return df_vol




def nsdl_data():
    df_data = pd.DataFrame()
    df = pd.read_html(html_file)

    df_idxfut = pd.DataFrame(df[df['Debt/Debt-VRR/Equity/Hybrid'] == 'Index Futures'])
    df_idxopt = pd.DataFrame(df[df['Debt/Debt-VRR/Equity/Hybrid'] == 'Index Options'])
    df_stkfut = pd.DataFrame(df[df['Debt/Debt-VRR/Equity/Hybrid'] == 'Stock Futures'])
    df_stkopt = pd.DataFrame(df[df['Debt/Debt-VRR/Equity/Hybrid'] == 'Stock Options'])


    df_idxfut.drop(df_idxfut.tail(1).index, inplace=True)

    df_idxfut.reset_index(inplace=True)
    df_idxopt.reset_index(inplace=True)
    df_stkfut.reset_index(inplace=True)
    df_stkopt.reset_index(inplace=True)

    df_idxopt.drop(df_idxopt.filter(regex='Investments').columns, inplace=True, axis=1)
    df_stkfut.drop(df_stkfut.filter(regex='Investments').columns, inplace=True, axis=1)
    df_stkfut.drop(df_stkfut.filter(regex='Investments').columns, inplace=True, axis=1)

    df_idxfut = df_idxfut.join(df_idxopt, how='left', lsuffix='_idxfut', rsuffix='_idxopt')
    df_stkfut = df_stkfut.join(df_stkopt, how='left', lsuffix='_stkfut', rsuffix='_stkopt')
    df_final = df_idxfut.join(df_stkfut, how='left', lsuffix='_stkfut', rsuffix='_stkopt')
    df_final.drop(['index_idxfut', 'index_idxopt', 'index_stkfut', 'index_stkopt'], inplace=True, axis=1)
    df_final.columns = df_final.columns.str.replace(df_idxopt.filter(regex='Investments').columns, 'Date')
    sheet_fiidata.range("A2").options(index=False, headers=True).value = df_final
    return


def main():

    global df_listoi,df_listvol,df_oi,df_vol

    df_oifii = pd.DataFrame()
    df_oidii = pd.DataFrame()
    df_oipro = pd.DataFrame()
    df_oiclient = pd.DataFrame()

    df_volfii = pd.DataFrame()
    df_voldii = pd.DataFrame()
    df_volpro = pd.DataFrame()
    df_volclient = pd.DataFrame()

    flag = 'o'


    for single_date in daterange(startdate, enddate):

        if single_date.weekday() not in weekdays:
            datestring = single_date.strftime("%d%m%Y")
            print(datestring)
            if datestring in holidays():
                continue
            try:
                urlvol = "https://archives.nseindia.com/content/nsccl/fao_participant_vol_"+datestring+".csv"
                urloi = "https://archives.nseindia.com/content/nsccl/fao_participant_oi_"+datestring+".csv"
                print(urloi)
                roi = requests.get(url=urloi).content
                rvol = requests.get(url=urlvol).content

                df_oitemp = pd.read_csv(io.StringIO(roi.decode('utf8')), skiprows=1)
                df_voltemp = pd.read_csv(io.StringIO(rvol.decode('utf8')), skiprows=1)

                try:
                    df_listoi = json.loads(open(oi_filename).read())
                except Exception as error:
                    print("Error reading data. Error : {0}".format(error))
                    df_listoi = []
                if df_listoi:
                    df_oi = pd.DataFrame()

                    for item in df_listoi:
                        df_oi = pd.concat([df_oi, pd.DataFrame(item)])

                try:
                    df_listvol = json.loads(open(vol_filename).read())
                except Exception as error:
                    print("Error reading data. Error : {0}".format(error))
                    df_listvol = []
                if df_listvol:
                    df_vol = pd.DataFrame()
                    for item in df_listvol:
                        df_vol = pd.concat([df_vol, pd.DataFrame(item)])

                df_oi = smartmoney(df_oitemp,df_oi,df_vol,flag='oi',dateindex=single_date.strftime("%d-%m-%Y"))
                df_vol = smartmoney(df_voltemp,df_oi,df_vol,flag='vol',dateindex=single_date.strftime("%d-%m-%Y"))

                # Participant wise open interest  data


                df_oi = df_oi.filter(['Index-conclusionoi', 'IndexLongoi', 'IndexShortoi', 'NetFuturesStocksoi', 'NetFuturesoi', 'Stock-conclusionoi', 'StockLongoi', 'StockShortoi','client type'])
                df_vol = df_vol.filter(['Index-conclusionvol', 'IndexLongvol', 'IndexShortvol', 'NetFuturesStocksvol', 'NetFuturesvol', 'Stock-conclusionvol', 'StockLongvol', 'StockShortvol','client type'])

                df_vol= df_vol[df_vol['client type'].isin(clientType)]
                df_oi= df_oi[df_oi['client type'].isin(clientType)]

                df_oifii1 = df_oi[df_oi['client type'] == 'FII']
                df_oifii1['Date'] = single_date.strftime("%d-%m-%Y")
                df_oifii = pd.concat([df_oifii,df_oifii1])

                df_oidii1 = df_oi[df_oi['client type'] == 'DII']
                df_oidii1['Date'] = single_date.strftime("%d-%m-%Y")
                df_oidii = pd.concat([df_oidii, df_oidii1])

                df_oipro1 = df_oi[df_oi['client type'] == 'Pro']
                df_oipro1['Date'] = single_date.strftime("%d-%m-%Y")
                df_oipro = pd.concat([df_oipro, df_oipro1])

                df_oiclient1 = df_oi[df_oi['client type'] == 'Client']
                df_oiclient1['Date'] = single_date.strftime("%d-%m-%Y")
                df_oiclient = pd.concat([df_oiclient, df_oiclient1])

                # Participant wise volume  data
                df_volfii1 = df_vol[df_vol['client type'] == 'FII']
                df_volfii=pd.concat([df_volfii,df_volfii1])

                df_voldii1 = df_vol[df_vol['client type'] == 'DII']
                df_voldii = pd.concat([df_voldii, df_voldii1])

                df_volpro1 = df_vol[df_vol['client type'] == 'Pro']
                df_volpro = pd.concat([df_volpro, df_volpro1])

                df_volclient1 = df_vol[df_vol['client type'] == 'Client']
                df_volclient = pd.concat([df_volclient, df_volclient1])

            except Exception as error:
                print("error {0}".format(error))
                continue

    df_fii = df_oifii.join(df_volfii, how='left', lsuffix='_OI', rsuffix='_VOL')
    df_fii.columns = df_fii.columns.str.replace(' ', '_')



    df_fii=df_fii[::-1]
    sheet_FII.range("A2").options(index=False, headers=True).value = df_fii[['Date','NetFuturesoi','NetFuturesStocksoi','IndexLongoi','IndexLongvol',
    'IndexShortoi','IndexShortvol','Index-conclusionoi','Index-conclusionvol','StockLongoi','StockLongvol',
    'StockShortoi','StockShortvol','Stock-conclusionoi','Stock-conclusionvol']]


    df_dii = df_oidii.join(df_voldii, how='left', lsuffix='_OI', rsuffix='_VOL')
    df_dii.columns = df_dii.columns.str.replace(' ', '_')
    df_dii = df_dii[::-1]

    sheet_DII.range("A2").options(index=False, headers=True).value = df_dii[['Date','NetFuturesoi','NetFuturesStocksoi','IndexLongoi','IndexLongvol',
    'IndexShortoi','IndexShortvol','Index-conclusionoi','Index-conclusionvol','StockLongoi','StockLongvol',
    'StockShortoi','StockShortvol','Stock-conclusionoi','Stock-conclusionvol']]

    df_pro = df_oipro.join(df_volpro, how='left', lsuffix='_OI', rsuffix='_VOL')
    df_pro.columns = df_pro.columns.str.replace(' ', '_')
    df_pro = df_pro[::-1]

    sheet_Pro.range("A2").options(index=False, headers=True).value = df_pro[['Date','NetFuturesoi','NetFuturesStocksoi','IndexLongoi','IndexLongvol',
    'IndexShortoi','IndexShortvol','Index-conclusionoi','Index-conclusionvol','StockLongoi','StockLongvol',
    'StockShortoi','StockShortvol','Stock-conclusionoi','Stock-conclusionvol']]



    df_client = df_oiclient.join(df_volclient, how='left', lsuffix='_OI', rsuffix='_VOL')
    df_client.columns = df_client.columns.str.replace(' ', '_')
    df_client = df_client[::-1]
    sheet_Client.range("A2").options(index=False, headers=True).value = df_client[['Date','NetFuturesoi','NetFuturesStocksoi','IndexLongoi','IndexLongvol',
    'IndexShortoi','IndexShortvol','Index-conclusionoi','Index-conclusionvol','StockLongoi','StockLongvol',
    'StockShortoi','StockShortvol','Stock-conclusionoi','Stock-conclusionvol']]


    df_fii['FuturesoiSum_FII'] = df_fii['NetFuturesoi'].sum()
    df_fii['FuturesvolSum_FII'] = df_fii['NetFuturesvol'].sum()
    df_fii['IndexLongoiSum_FII'] = df_fii['IndexLongoi'].sum()
    df_fii['IndexShortoiSum_FII'] = df_fii['IndexShortoi'].sum()
    df_fii['IndexLongvolSum_FII'] = df_fii['IndexLongvol'].sum()
    df_fii['IndexShortvolSum_FII'] = df_fii['IndexShortvol'].sum()

    df_fii['StockLongoiSum_FII'] = df_fii['IndexLongoi'].sum()
    df_fii['StockShortoiSum_FII'] = df_fii['IndexShortoi'].sum()
    df_fii['StockLongvolSum_FII'] = df_fii['IndexLongvol'].sum()
    df_fii['StockShortvolSum_FII'] = df_fii['IndexShortvol'].sum()

    df_fii['conclusionIndexFII'] = np.where((df_fii['IndexLongoiSum_FII'] >= df_fii['IndexShortoiSum_FII']),
                                               'Bullish', 'Bearish')
    df_fii['conclusionStockFII'] = np.where((df_fii['StockLongoiSum_FII'] >= df_fii['StockShortoiSum_FII']),
                                               'Bullish', 'Bearish')

    df_fii = df_fii.filter(
        ['FuturesoiSum_FII', 'FuturesvolSum_FII', 'IndexLongoiSum_FII', 'IndexShortoiSum_FII', 'IndexLongvolSum_FII',
         'IndexShortvolSum_FII', 'StockLongoiSum_FII', 'StockShortoiSum_FII', 'StockLongvolSum_FII',
         'StockShortvolSum_FII', 'conclusionIndexFII', 'conclusionStockFII'])

    # Cummulative  DII Futures calculation
    df_dii['FuturesoiSum_DII'] = df_dii['NetFuturesoi'].sum()
    df_dii['FuturesvolSum_DII'] = df_dii['NetFuturesvol'].sum()

    # Cummulative  DII Index calculation
    df_dii['IndexLongoiSum_DII'] = df_dii['IndexLongoi'].sum()
    df_dii['IndexShortoiSum_DII'] = df_dii['IndexShortoi'].sum()
    df_dii['IndexLongvolSum_DII'] = df_dii['IndexLongvol'].sum()
    df_dii['IndexShortvolSum_DII'] = df_dii['IndexShortvol'].sum()

    # Cummulative DII Stock calculation

    df_dii['StockLongoiSum_DII'] = df_dii['IndexLongoi'].sum()
    df_dii['StockShortoiSum_DII'] = df_dii['IndexShortoi'].sum()
    df_dii['StockLongvolSum_DII'] = df_dii['IndexLongvol'].sum()
    df_dii['StockShortvolSum_DII'] = df_dii['IndexShortvol'].sum()

    df_dii['conclusionIndexDII'] = np.where((df_dii['IndexLongoiSum_DII'] >= df_dii['IndexShortoiSum_DII']), 'Bullish','Bearish')
    df_dii['conclusionStockDII'] = np.where((df_dii['StockLongoiSum_DII'] >= df_dii['StockShortoiSum_DII']), 'Bullish','Bearish')
    df_dii = df_dii.filter(
        ['FuturesoiSum_DII', 'FuturesvolSum_DII', 'IndexLongoiSum_DII', 'IndexShortoiSum_DII', 'IndexLongvolSum_DII',
         'IndexShortvolSum_DII', 'StockLongoiSum_DII', 'StockShortoiSum_DII', 'StockLongvolSum_DII',
         'StockShortvolSum_DII','conclusionIndexDII', 'conclusionStockDII'])

    # Cummulative  PRO Futures calculation
    df_pro['FuturesoiSum_PRO'] = df_pro['NetFuturesoi'].sum()
    df_pro['FuturesvolSum_PRO'] = df_pro['NetFuturesvol'].sum()

    # Cummulative  PRO Index calculation
    df_pro['IndexLongoiSum_PRO'] = df_pro['IndexLongoi'].sum()
    df_pro['IndexShortoiSum_PRO'] = df_pro['IndexShortoi'].sum()
    df_pro['IndexLongvolSum_PRO'] = df_pro['IndexLongvol'].sum()
    df_pro['IndexShortvolSum_PRO'] = df_pro['IndexShortvol'].sum()

    # Cummulative PRO Stock calculation

    df_pro['StockLongoiSum_PRO'] = df_pro['IndexLongoi'].sum()
    df_pro['StockShortoiSum_PRO'] = df_pro['IndexShortoi'].sum()
    df_pro['StockLongvolSum_PRO'] = df_pro['IndexLongvol'].sum()
    df_pro['StockShortvolSum_PRO'] = df_pro['IndexShortvol'].sum()
    df_pro['conclusionIndexPRO'] = np.where((df_pro['IndexLongoiSum_PRO'] >= df_pro['IndexShortoiSum_PRO']), 'Bullish','Bearish')
    df_pro['conclusionStockPRO'] = np.where((df_pro['StockLongoiSum_PRO'] >= df_pro['StockShortoiSum_PRO']), 'Bullish','Bearish')

    df_pro = df_pro.filter(
        ['FuturesoiSum_PRO', 'FuturesvolSum_PRO', 'IndexLongoiSum_PRO', 'IndexShortoiSum_PRO', 'IndexLongvolSum_PRO',
         'IndexShortvolSum_PRO','StockLongoiSum_PRO', 'StockShortoiSum_PRO', 'StockLongvolSum_PRO',
         'StockShortvolSum_PRO', 'conclusionIndexPRO', 'conclusionStockPRO'])

    # Cummulative  Client Futures calculation
    df_client['FuturesoiSum_CLIENT'] = df_client['NetFuturesoi'].sum()
    df_client['FuturesvolSum_CLIENT'] = df_client['NetFuturesvol'].sum()

    # Cummulative  Client Index calculation
    df_client['IndexLongoiSum_CLIENT'] = df_client['IndexLongoi'].sum()
    df_client['IndexShortoiSum_CLIENT'] = df_client['IndexShortoi'].sum()
    df_client['IndexLongvolSum_CLIENT'] = df_client['IndexLongvol'].sum()
    df_client['IndexShortvolSum_CLIENT'] = df_client['IndexShortvol'].sum()

    # Cummulative Client Stock calculation

    df_client['StockLongoiSum_CLIENT'] = df_client['IndexLongoi'].sum()
    df_client['StockShortoiSum_CLIENT'] = df_client['IndexShortoi'].sum()
    df_client['StockLongvolSum_CLIENT'] = df_client['IndexLongvol'].sum()
    df_client['StockShortvolSum_CLIENT'] = df_client['IndexShortvol'].sum()

    df_client['conclusionIndexCLIENT'] = np.where((df_client['IndexLongoiSum_CLIENT'] >= df_client['IndexShortoiSum_CLIENT']), 'Bullish','Bearish')
    df_client['conclusionStockCLIENT'] = np.where((df_client['StockLongoiSum_CLIENT'] >= df_client['StockShortoiSum_CLIENT']), 'Bullish','Bearish')

    df_client = df_client.filter(
        ['FuturesoiSum_CLIENT', 'FuturesvolSum_CLIENT', 'IndexLongoiSum_CLIENT', 'IndexShortoiSum_CLIENT', 'IndexLongvolSum_CLIENT',
         'IndexShortvolSum_CLIENT', 'StockLongoiSum_CLIENT', 'StockShortoiSum_CLIENT', 'StockLongvolSum_CLIENT',
         'StockShortvolSum_CLIENT','conclusionIndexCLIENT', 'conclusionStockCLIENT'])



    wb.sheets['Dashboard'].range("A30").options(index=False, header=True).value = 'Client Type'
    wb.sheets['Dashboard'].range("A31").options(index=False, header=True).value = 'FII'
    wb.sheets['Dashboard'].range("B30").options(index=False, header=True).value = df_fii.iloc[0:]
    wb.sheets['Dashboard'].range("A32").options(index=False, header=False).value = 'DII'
    wb.sheets['Dashboard'].range("B32").options(index=False, header=False).value = df_dii.iloc[0:]
    wb.sheets['Dashboard'].range("A33").options(index=False, header=False).value = 'Client'
    wb.sheets['Dashboard'].range("B33").options(index=False, header=False).value = df_client.iloc[0:]
    wb.sheets['Dashboard'].range("A34").options(index=False, header=False).value = 'Pro'
    wb.sheets['Dashboard'].range("B34").options(index=False, header=False).value = df_pro.iloc[0:]


if __name__ == '__main__':
    main()
