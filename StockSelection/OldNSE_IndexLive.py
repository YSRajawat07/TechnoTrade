import pandas as pd
import xlwings as xw
from datetime import datetime, time, timedelta
import requests
import json
import numpy as np
from time import sleep
import os


excel_file = "/Users/ajaysinghrajawat/Documents/pythonprojects/Files/NSE_OptionChainwip.xlsx"
wb = xw.Book(excel_file)
sheet_oi_single = wb.sheets("OIData")
sht_live = wb.sheets("Data")

oi_filename = os.path.join("/Users/ajaysinghrajawat/Documents/pythonprojects/Files/Intraday/Daily",
                           "OINSEwip_data_records_{0}".format(datetime.now().strftime("%d%m%y")))
mp_filename = os.path.join("/Users/ajaysinghrajawat/Documents/pythonprojects/Files/Intraday/Daily",
                           "MPNSEwip_data_records_{0}".format(datetime.now().strftime("%d%m%y")))

expiry = "14MAY2020"

df_list = []
mp_list = []

#  span = n largest open interest values
span = 5
timeframe = 10

pd.set_option('display.width', 1500)
pd.set_option('display.max_columns', 75)
pd.set_option('display.max_rows', 1500)


# Encoder to avoid error :typeerror object of type int64 is not json serializable pandas

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


def getoptionchain(df, mp_df, df_cdata, df_pdata):
    tries = 1
    max_retries = 3
    while tries <= max_retries:
        try:
            headers = {
                "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                              'Chrome/80.0.3987.100 Safari/537.36',
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate",
                "Referer": "https://www1.nseindia.com/products/content/equities/equities/eq_security.htm"}

            url = (
                    "https://www1.nseindia.com/live_market/dynaContent/live_watch/option_chain/optionKeys.jsp?symbolCode=-"
                    "10006&symbol=NIFTY&symbol=NIFTY&instrument=-&date=" + expiry + "&segmentLink=17&symbolCount=2&segmentLink=17")

            url_fut = "https://www1.nseindia.com/live_market/dynaContent/live_watch/fomwatchsymbol.jsp?key=NIFTY&Fut_Opt=Futures"

            r = requests.get(url, headers=headers).text

            # fut = requests.get(url_fut, headers=headers).text
            #
            # nsefut = pd.read_html(fut)
            # flist = nsefut[1]
            # futstr = nsefut[0][1]



            nseoption = pd.read_html(r)

            dlist = nseoption[1]
            niftystr = nseoption[0][1]
            str = " "
            strlist = list(niftystr)
            str = str.join(strlist)
            niftyDate = str[-25:]
            niftyspot = str[24:32]



            # if len(df_list) > 0:
            #     df['Time'] = df_list[-1][0]['Time']


            if not df.empty and df['Underlying'].iloc[-1] == niftyspot:
                print('Duplicate data, not recording')
                sleep(10)
                tries += 1
                continue

            df.reset_index(inplace=True)
            df1 = pd.DataFrame(dlist['Unnamed: 11_level_0'])
            df2 = pd.DataFrame(dlist['CALLS'])
            df3 = pd.DataFrame(dlist['PUTS'])
            df1.columns = df1.columns.str.replace(' ', '_')
            df3.columns = df3.columns.str.replace(' ', '_')
            df2.columns = df2.columns.str.replace(' ', '_')

            df2 = df2.join(df1)

            df = df2.join(df3, how='left', lsuffix='_Call', rsuffix='_Put')

            df['callsOTM'] = (df.Strike_Price > round(float(niftyspot), 0))
            df['putsOTM'] = (df.Strike_Price < round(float(niftyspot), 0))

            df = df.replace(to_replace="-", value=0)

            sheet_oi_single.range("A1").options(header=True, index=False).value = df.drop(['Chart_Call', 'Chart_Put'],
                                                                                          axis=1)
            df['OI_Call'] = df['OI_Call'].astype(int)

            df['Chng_in_OI_Call'] = pd.to_numeric(df['Chng_in_OI_Call'])

            df['Volume_Call'] = df['Volume_Call'].astype(int)
            df['OI_Put'] = df['OI_Put'].astype(int)
            df['Chng_in_OI_Put'] = pd.to_numeric(df['Chng_in_OI_Put'])

            df['Volume_Put'] = df['Volume_Put'].astype(int)

            # if len(df_list) > 0:
            #     df1['Time'] = df_list[-1][0]['Time']


            df['Time'] = datetime.now().strftime("%H:%M")

            ivotmput = (df[df['putsOTM'] == True])['IV_Put'].tail(1)
            ivotmcall = (df[df['callsOTM'] == True])['IV_Call'].head(1)



            callDecay = (df[df['callsOTM'] == True]).nlargest(span, 'Chng_in_OI_Call', keep='last')['LTP_Call'].astype(float)
            callOI = (df[df['callsOTM'] == True]).nlargest(span, 'Chng_in_OI_Call', keep='last')['OI_Call']

            callvol = (df[df['callsOTM'] == True]).nlargest(span, 'Chng_in_OI_Call', keep='last')['Volume_Call']
            callCHOI = (df[df['callsOTM'] == True]).nlargest(span, 'Chng_in_OI_Call', keep='last')['Chng_in_OI_Call']
            IVdatacall = (df[df['callsOTM'] == True]).nlargest(span, 'Chng_in_OI_Call', keep='last')['IV_Call'].astype(float)
            callstrikepice = (df[df['callsOTM'] == True]).nlargest(span, 'Chng_in_OI_Call', keep='last')['Strike_Price']

            pcr = (df[df['putsOTM'] == True].nlargest(span, 'Chng_in_OI_Put', keep='last')['OI_Put'].sum()) / \
                  (df[df['callsOTM'] == True].nlargest(span, 'Chng_in_OI_Call', keep='last')['OI_Call'].sum())

            PutDecay = (df[df['putsOTM'] == True]).nlargest(span, 'Chng_in_OI_Put', keep='last')['LTP_Put'].astype(float)
            putOI = (df[df['putsOTM'] == True]).nlargest(span, 'Chng_in_OI_Put', keep='last')['OI_Put']

            putvol = (df[df['putsOTM'] == True]).nlargest(span, 'Chng_in_OI_Put', keep='last')['Volume_Put']
            putCHOI = (df[df['putsOTM'] == True]).nlargest(span, 'Chng_in_OI_Put', keep='last')['Chng_in_OI_Put']
            IVdataput = (df[df['putsOTM'] == True]).nlargest(span, 'Chng_in_OI_Put', keep='last')['IV_Put'].astype(float)
            putstrikeprice = (df[df['putsOTM'] == True]).nlargest(span, 'Chng_in_OI_Put', keep='last')['Strike_Price']


            # Max pain calculation starts here

            df['sumOI'] = df['Chng_in_OI_Call'] + df['Chng_in_OI_Put']

            df['Underlying'] = niftyspot

            maxpain = df.nlargest(3, 'sumOI', keep='last')['Strike_Price']

            df_call = pd.DataFrame(callOI)
            df_put = pd.DataFrame(putOI)
            df_call = df_call.join(callCHOI)
            df_call = df_call.join(callvol)
            df_call = df_call.join(callDecay)
            df_call = df_call.join(IVdatacall)
            df_call = df_call.join(callstrikepice)

            df_put = df_put.join(putCHOI)
            df_put = df_put.join(putvol)
            df_put = df_put.join(PutDecay)
            df_put = df_put.join(IVdataput)
            df_put = df_put.join(putstrikeprice)

            df_cdata = pd.concat([df_cdata, df_call])

            df_put.reset_index(drop=True, inplace=True)

            df_pdata = pd.concat([df_pdata, df_put])
            df_call.reset_index(drop=True, inplace=True)

            df_data = df_call.join(df_put, how='left', lsuffix='_Call', rsuffix='_Put')



            #####Interpretation on first span hightest OI strikes OI################


            mp_dict = {datetime.now().strftime("%H:%M"): {'underlying': niftyspot,
                                                          'MaxPain': maxpain.iloc[0],
                                                          'PCR': pcr,
                                                          'OI_Call': df_data['OI_Call'].mean(),
                                                          'Chng_in_OI_Call': df_data['Chng_in_OI_Call'].mean(),
                                                          'Volume_Call': df_data['Volume_Call'].mean(),
                                                          'LTP_Call': df_data['LTP_Call'].mean(),
                                                          'IV_Call': ivotmcall.iloc[0],
                                                          'OI_Put': df_data['OI_Put'].mean(),
                                                          'Chng_in_OI_Put': df_data['Chng_in_OI_Put'].mean(),
                                                          'Volume_Put': df_data['Volume_Put'].mean(),
                                                          'LTP_Put': df_data['LTP_Put'].mean(),
                                                          'IV_Put': ivotmput.iloc[0]
                                                          }}






            df4 = pd.DataFrame(mp_dict).transpose()

            mp_df = pd.concat([mp_df, df4], sort=False)

            df_mpdata = (mp_df.apply(pd.to_numeric).diff())

            # Bullish senario for first highest prices

            df_mpdata['LongInCall1'] = np.where(
                ((df_mpdata['PCR'] < 0) & (df_mpdata['OI_Call'] > 0) & (df_mpdata['IV_Call'] >= 0)), 'True', 'False')

            df_mpdata['ShortInPut1'] = np.where(
                ((df_mpdata['PCR'] > 0) & (df_mpdata['OI_Put'] > 0) & (df_mpdata['IV_Put'] <=0)), 'True', 'False')
            df_mpdata['ShortCoveringCall1'] = np.where(
                ((df_mpdata['PCR'] > 0) & (df_mpdata['OI_Call'] < 0) & (df_mpdata['IV_Call'] >= 0)), 'True', 'False')
            df_mpdata['UnwindingPut1'] = np.where(
                ((df_mpdata['PCR'] < 0) & (df_mpdata['OI_Put'] < 0) & (df_mpdata['IV_Put'] <= 0)), 'True', 'False')

            df_mpdata['Bullish'] = np.where(((df_mpdata['LongInCall1'] == 'True') & (df_mpdata['ShortInPut1'] == 'True') |
                                             (df_mpdata['LongInCall1'] == 'True') & (
                                                     df_mpdata['ShortCoveringCall1'] == 'True') |
                                             (df_mpdata['LongInCall1'] == 'True') & (df_mpdata['UnwindingPut1'] == 'True') |
                                             (df_mpdata['ShortInPut1'] == 'True') & (
                                                     df_mpdata['ShortCoveringCall1'] == 'True') |
                                             (df_mpdata['ShortCoveringCall1'] == 'True') & (
                                                     df_mpdata['UnwindingPut1'] == 'True')), 'True', 'False')

            # Bearish senario for first highest prices

            df_mpdata['LongInPut1'] = np.where(
                ((df_mpdata['PCR'] > 0) & (df_mpdata['OI_Put'] > 0) & (df_mpdata['IV_Put'] >= 0)), 'True', 'False')
            df_mpdata['ShortInCall1'] = np.where(
                ((df_mpdata['PCR'] < 0) & (df_mpdata['OI_Call'] > 0) & (df_mpdata['IV_Call'] <= 0)), 'True', 'False')
            df_mpdata['ShortCoveringPut1'] = np.where(
                ((df_mpdata['PCR'] < 0) & (df_mpdata['OI_Put'] < 0) & (df_mpdata['IV_Put'] >= 0)), 'True', 'False')
            df_mpdata['UnwindingCall1'] = np.where(
                ((df_mpdata['PCR'] > 0) & (df_mpdata['OI_Call'] < 0) & (df_mpdata['IV_Call'] <= 0)), 'True', 'False')

            df_mpdata['Bearish'] = np.where(((df_mpdata['LongInPut1'] == 'True') & (df_mpdata['ShortInCall1'] == 'True') |
                                             (df_mpdata['LongInPut1'] == 'True') & (df_mpdata['ShortCoveringPut1'] == 'True') |
                                             (df_mpdata['LongInPut1'] == 'True') & (df_mpdata['UnwindingCall1'] == 'True') |
                                             (df_mpdata['ShortInCall1'] == 'True') & (df_mpdata['ShortCoveringPut1'] == 'True') |
                                             (df_mpdata['ShortCoveringPut1'] == 'True') & (df_mpdata['UnwindingCall1'] == 'True')), 'True', 'False')

            df_mpdata['Buy'] = np.where((df_mpdata['Bearish'] == 'False') & (df_mpdata['Bullish'] == 'True'), 'Yes', 'No')
            df_mpdata['Sell'] = np.where((df_mpdata['Bearish'] == 'True') & (df_mpdata['Bullish'] == 'False'), 'Yes', 'No')



            wb.sheets['Dashboard'].range("A40").options(index=False, header=False).value = df_data
            wb.sheets['Dashboard'].range("J1").options(index=False, header=False).value = maxpain



            wb.sheets['Dashboard'].range("C2").options(index=False, header=False).value=df_mpdata['Buy'].iloc[0]
            wb.sheets['Dashboard'].range("D2").options(index=False, header=False).value=df_mpdata['Sell'].iloc[0]
            wb.sheets['Dashboard'].range("G2").options(index=False, header=False).value=datetime.now().strftime("%H:%M")
            wb.sheets['Dashboard'].range("G1").options(index=False, header=False).value=niftyspot

            df_corr = df_mpdata.corr()[['underlying', 'LTP_Call', 'LTP_Put']]
            df_corr.reset_index()
            df_corr = df_corr.transpose()





            wb.sheets['Dashboard'].range("B35").options(index=False, header=False).value = df_corr.drop(
                ['Volume_Call', 'Volume_Put', 'underlying', 'MaxPain', 'OI_Call', 'OI_Put'], axis=1)

            wb.sheets['MPData'].range("A2").options(header=False).value = mp_df.iloc[::-1]
            wb.sheets['TrendData'].range("A2").options(header=False).value = df_mpdata.iloc[::-1]

            with open(mp_filename, "w") as files:
                files.write(json.dumps(mp_df.to_dict(), cls=NpEncoder, indent=4, sort_keys=True))
                # wb.api.RefreshAll()

            df_list.append(df.to_dict('records'))

            with open(oi_filename, "w") as files:
                files.write(json.dumps(df_list, indent=4, sort_keys=True))

            return df, mp_df, df_cdata, df_pdata

        except Exception as error:
            print('before error')
            print("error {0}".format(error))

            tries += 1
            sleep(10)
            continue
    if tries >= max_retries:
        print("Max retries exceeded. No new data time {0}".format(datetime.now()))
        return df, mp_df, df_cdata, df_pdata


def main():
    global df_list
    df_mpdata = pd.DataFrame()
    df_cdata = pd.DataFrame()
    df_pdata = pd.DataFrame()

    try:
        df_list = json.loads(open(oi_filename).read())
    except Exception as error:
        print("Error reading data. Error : {0}".format(error))
        df_list = []

    if df_list:
        df = pd.DataFrame()

        for item in df_list:
            df = pd.concat([df, pd.DataFrame(item)])
    else:
        df = pd.DataFrame()

    try:
        mp_list = json.loads(open(mp_filename).read())
        # mp_list = ast.literal_eval(mp_list)
        mp_df = pd.DataFrame().from_dict(mp_list)

    except Exception as error:

        print("Error reading data. Error : {0}".format(error))
        mp_list = []
        mp_df = pd.DataFrame()
        mp_data = pd.DataFrame()


    if not time(9,15) <= datetime.now().time() <= time(15,32):
        print("Market is off as of now, please run during market hours")

    while time(9, 15) <= datetime.now().time() <= time(15, 32):

        timenow = datetime.now()
        check = True if round(int(str(timenow.minute)) / timeframe) in list(np.arange(0.00, 10.00)) else False
        print(round(int(str(timenow.minute)) / timeframe))
        print(check)

        if check:
            nextscan = timenow + timedelta(minutes=timeframe)
            df, mp_df, df_cdata, df_pdata = getoptionchain(df, mp_df, df_cdata, df_pdata)


        if not df.empty:
            df['IV_Call'] = df['IV_Call'].replace(to_replace=0, method='bfill').values
            df['IV_Put'] = df['IV_Put'].replace(to_replace=0, method='bfill').values
            df['datetime'] = datetime.now().strftime("%d-%m-%y%y %H:%M")

            sht_live.range("A2").options(header=True, index=False).value = df

            # wb.api.RefreshAll()
            nextscan = timenow + timedelta(minutes=timeframe)
            waitsecs = int((nextscan - datetime.now()).seconds)
            print("Wait for {0} seconds".format(waitsecs))
            sleep(waitsecs) if waitsecs > 0 else sleep(0)

        else:
            print("No data received")
            sleep(30)


if __name__ == '__main__':
    main()
