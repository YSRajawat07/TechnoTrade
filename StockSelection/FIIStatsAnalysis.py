import pandas as pd
import xlwings as xw
from datetime import date, datetime, time, timedelta

import requests,os,json


# Declaration of files and display setting for panda

html_file= "/Users/ajaysinghrajawat/Documents/pythonprojects/Files/fii.html"
excel_file = "/Users/ajaysinghrajawat/Documents/pythonprojects/Files/IndexAnalysis.xlsx"
oi_filename = os.path.join("/Users/ajaysinghrajawat/Documents/pythonprojects/Files/IndexData", "OINSE_data_{0}".format(datetime.now().strftime("%m%y")))
wb = xw.Book(excel_file)

df_listoi=[]

holidays=['02-Apr-2020','06-Apr-2020','10-Apr-2020','14-Apr-2020','01-May-2020','25-May-2020']

weekdays = [5, 6]

df_oi = pd.DataFrame()


sheet_fiidata = wb.sheets("FIIData")
sheet_net = wb.sheets("OptionChain")

start_date = date(2020, 7, 10)
end_date = date(2020, 7, 17)

pd.set_option('display.width', 1500)
pd.set_option('display.max_columns', 75)
pd.set_option('display.max_rows', 10000)



def smartmoney(df,df_oi):


    df['NetContracts'] = (df.ContractsBuy - df.ContractsSell)
    df['NetAmountInCorers'] = (df.AmountBuy - df.AmountSell)

    df_oi = pd.concat([df_oi, df])

    # df_listoi.append(df.to_dict())

    # with open(oi_filename, "w") as files:
    #     files.write(json.dumps(df_listoi, indent=4, sort_keys=True))
    return df_oi

def daterange(start_date, end_date):
    print('inside')
    for n in range(int((end_date - start_date).days) + 1):
       yield start_date + timedelta(n)


def main():

    global df_listoi,df_listvol,df_oi,df_vol

    df_idxfut = pd.DataFrame()
    df_idxopt = pd.DataFrame()
    df_idxstk = pd.DataFrame()
    df_futstk = pd.DataFrame()



    for single_date in daterange(start_date, end_date):

        if single_date.weekday() not in weekdays:

            datestring = single_date.strftime("%d-%b-%Y")
            if datestring in holidays:
                continue

            print(datestring)

            try:

                urloi = "https://www1.nseindia.com/content/fo/fii_stats_"+datestring+".xls"

                roi = requests.get(url=urloi).content

                df_oitemp = pd.read_excel(roi,skiprows=2,skipfooter=13)
                df_oitemp = pd.DataFrame(df_oitemp)

                df_oitemp.rename(columns={'Unnamed: 0': 'Type'}, inplace=True)
                df_oitemp.rename(columns={'No. of contracts': 'ContractsBuy'}, inplace=True)
                df_oitemp.rename(columns={'Amt in Crores': 'AmountBuy'}, inplace=True)
                df_oitemp.rename(columns={'No. of contracts.1': 'ContractsSell'}, inplace=True)
                df_oitemp.rename(columns={'Amt in Crores.1': 'AmountSell'}, inplace=True)
                df_oitemp.drop('No. of contracts.2',inplace=True,axis=1)
                df_oitemp.drop('Amt in Crores.2', inplace=True,axis=1)
                df_oitemp['Date'] =datestring

                # try:
                #     df_listoi = json.loads(open(oi_filename).read())
                # except Exception as error:
                #     print("Error reading data. Error : {0}".format(error))
                #     df_listoi = []
                # if df_listoi:
                #     df_oi = pd.DataFrame()
                #     for item in df_listoi:
                #         df_oi = pd.concat([df_oi, pd.DataFrame(item)])

                df_oitemp['NetContracts'] = (df_oitemp.ContractsBuy - df_oitemp.ContractsSell)
                df_oitemp['NetAmountInCorers'] = (df_oitemp.AmountBuy - df_oitemp.AmountSell)

                df_oi = pd.concat([df_oi, df_oitemp])
            except Exception as error:
                df_oitemp = pd.DataFrame()
                print("error {0}".format(error))
                continue

    # Participant wise open interest  data

    df_idxfut1 = df_oi[df_oi['Type'] == 'INDEX FUTURES']
    df_idxfut = pd.concat([df_idxfut,df_idxfut1])

    df_idxopt1= df_oi[df_oi['Type'] == 'INDEX OPTIONS']
    df_idxopt = pd.concat([df_idxopt, df_idxopt1])

    df_idxstk1 = df_oi[df_oi['Type'] == 'STOCK FUTURES']
    df_idxstk = pd.concat([df_idxstk, df_idxstk1])

    df_futstk1 = df_oi[df_oi['Type'] == 'STOCK OPTIONS']
    df_futstk = pd.concat([df_futstk, df_futstk1])


    sheet_fiidata.range("A2").options(index=True, headers=False).value = df_idxfut[['Date','ContractsBuy','AmountBuy','ContractsSell','AmountSell','NetContracts','NetAmountInCorers']]
    sheet_fiidata.range("A2").options(index=True, headers=False).value = df_idxopt[['ContractsBuy','AmountBuy','ContractsSell','AmountSell','NetContracts','NetAmountInCorers']]
    sheet_fiidata.range("A2").options(index=True, headers=False).value = df_idxstk[['ContractsBuy','AmountBuy','ContractsSell','AmountSell','NetContracts','NetAmountInCorers']]
    sheet_fiidata.range("A2").options(index=True, headers=False).value = df_futstk[['ContractsBuy','AmountBuy','ContractsSell','AmountSell','NetContracts','NetAmountInCorers']]

    sheet_net.range("A100").options(index=False, headers=False).value = df_idxfut[['Date','NetContracts','NetAmountInCorers']]
    sheet_net.range("D100").options(index=False, headers=False).value = df_futstk[['NetContracts','NetAmountInCorers']]
    sheet_net.range("F100").options(index=False, headers=False).value = df_idxopt[['NetContracts','NetAmountInCorers']]
    sheet_net.range("H100").options(index=False, headers=False).value = df_idxstk[['NetContracts','NetAmountInCorers']]



    print(df_oi)

if __name__ == '__main__':
    main()
