from scipy.stats import norm as stats
from NseDataFetch import *
from NseATMCalculations1 import *
from datetime import datetime,timedelta,time
import json
import configparser
# Declaration of files and display setting for panda

config = configparser.RawConfigParser()
config.read('ConfigFile.properties')

# path =config.get('filedetailswip', 'filelocationwip')
# filename = config.get('filedetailswip','NseIndexliveWIP.filename')

path =config.get('filedetails', 'filelocation')
filename = config.get('filedetails','NseIndexlive.filename')

excel_file = path+filename
wb = xw.Book(excel_file)
sheet_oi_single = wb.sheets("OIData")
sht_live = wb.sheets("Data")

pd.set_option('mode.chained_assignment', None)

np.seterr(divide = 'ignore')

# oi_filename = os.path.join(path+"/Intraday/WIP",
#                            "nseindexlive_{0}".format(datetime.now().strftime("%d%m%y")))
# fut_filename = os.path.join(path+"/Intraday/WIP",
#                            "nsefutlive_{0}".format(datetime.now().strftime("%d%m%y")))
# mpcall_filename = os.path.join(path+"/Intraday/WIP",
#                            "nseindexcalllive_{0}".format(datetime.now().strftime("%d%m%y")))
# mpput_filename = os.path.join(path+"/Intraday/WIP",
#                            "nseindexputlive_{0}".format(datetime.now().strftime("%d%m%y")))
# niftyfuturerecords = os.path.join(path+"/EOD/backup",
#                                   "nsefuteod_{0}".format(datetime.now().strftime("%m%y")))

oi_filename = os.path.join(path+"/Intraday/Daily",
                           "nseindexlive_{0}".format(datetime.now().strftime("%d%m%y")))
fut_filename = os.path.join(path+"/Intraday/Daily",
                           "nsefutlive_{0}".format(datetime.now().strftime("%d%m%y")))
mpcall_filename = os.path.join(path+"/Intraday/Daily",
                           "nseindexcalllive_{0}".format(datetime.now().strftime("%d%m%y")))
mpput_filename = os.path.join(path+"/Intraday/Daily",
                           "nseindexputlive_{0}".format(datetime.now().strftime("%d%m%y")))
niftyfuturerecords = os.path.join(path+"/EOD/backup",
                                  "nsefuteod_{0}".format(datetime.now().strftime("%m%y")))


##Date format without 0 inn front for single digit day e.g "4JUN2020"
indexval=0

df_list = []
mp_list = []

mpdict_activecall=[]
mpdict_activeput=[]

#  span = n largest open interest values
span = 3
largestcall = 'openInterest_Call'
largestput = 'openInterest_Put'

timeframe =3
interval=20
start= time(00, 15)
end= time(23, 33)
startref= time(8,15)
endref= time(8,50 )

niftyspot = 0000.00
niftyvolume = 00000000

fromtime =datetime(2020, 7, 30, 15, 30)
totime =datetime(2020, 7, 30, 15, 30)
t1=totime-fromtime
daystoexpiry = t1 / timedelta(days=1)



pd.set_option('display.width', 1500)
pd.set_option('display.max_columns', 75)
pd.set_option('display.max_rows', 1500)

nifty = pd.DataFrame()

wb.sheets['FutureData'].range('2:50000').clear_contents()
wb.sheets['MPDataput'].range('2:50000').clear_contents()
wb.sheets['MPDatacall'].range('2:50000').clear_contents()
wb.sheets['CallData'].range('2:50000').clear_contents()
wb.sheets['PutData'].range('2:50000').clear_contents()
wb.sheets['zscore'].range('2:50000').clear_contents()
wb.sheets['NearATMDatacall'].range('2:50000').clear_contents()
#wb.sheets['NearATMDataput'].range('2:50000').clear_contents()
wb.sheets['CallDataATM'].range('2:50000').clear_contents()
wb.sheets['PutDataATM'].range('2:50000').clear_contents()


# Encoder to avoid error :typeerror object of type int64 is not json serializable pandas

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.bool_):
            return obj.tolist()
        elif hasattr(obj, 'to_json'):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)



def getoptionchain(df, mpcall_df,mpput_df, df_cdata, df_pdata, df_diff):

    df,ce_data,pe_data = niftyoptionchain()
    vix =  indiavix()
    vixspot = vix['last'].values
    nifty = niftyspotfun()
    niftyspot = nifty['lastPrice'].values
    wkhigh = np.round_(niftyspot * (100 + ((vixspot/np.sqrt(52))))/100,2)
    wklow = np.round_(niftyspot * (100 - ((vixspot/np.sqrt(52))))/100,2)

    dailyhigh = np.round_(niftyspot * (100 + ((vixspot/np.sqrt(365))))/100,2)
    dailylow = np.round_(niftyspot * (100 - ((vixspot/np.sqrt(365))))/100,2)

    high = np.round_(niftyspot * (100 + ((vixspot/np.sqrt(365*24*4))))/100,2)
    low = np.round_(niftyspot * (100 - ((vixspot/np.sqrt(365*24*4))))/100,2)

    wb.sheets['Dashboard'].range("J3").options(index=False, header=False).value = vixspot

    # ce_data['callsOTM'] = ce_data.strikePrice >= roundx(niftyspot)
    # pe_data['putsOTM'] = pe_data.strikePrice <= roundx(niftyspot)



    df['callsOTM'] = (df.strikePrice_Call >= roundx(niftyspot))
    df['putsOTM'] = (df.strikePrice_Put < roundx(niftyspot))

    print('processing option chain data.......')
    df['openInterest_Call'] = df['openInterest_Call'].astype(int)

    df['changeinOpenInterest_Call'] = pd.to_numeric(df['changeinOpenInterest_Call'])
    df['totalTradedVolume_Call'] = df['totalTradedVolume_Call'].astype(int)
    df['openInterest_Put'] = df['openInterest_Put'].astype(int)
    df['changeinOpenInterest_Put'] = pd.to_numeric(df['changeinOpenInterest_Put'])
    df['totalTradedVolume_Put'] = df['totalTradedVolume_Put'].astype(int)

    df['Time'] = datetime.now().strftime("%d-%m %H:%M")
    df = df[:-1]
    pcr = (df['openInterest_Put'].sum()) / \
          (df['openInterest_Put'].sum())

    pcr_vol = round((df['totalTradedVolume_Put'].sum() / df['totalTradedVolume_Call'].sum()), 2)

    wb.sheets['Dashboard'].range("G2").options(index=False, header=True).value = pcr
    wb.sheets['Dashboard'].range("G3").options(index=False, header=True).value = pcr_vol



    dfcallsatm = (df[df['callsOTM'] == True]).head(1)
    dfputsatm = dfcallsatm

    dfputsatm = dfputsatm.filter(
        [ 'Time','openInterest_Call','totalTradedVolume_Call','changeinOpenInterest_Call','impliedVolatility_Call','lastPrice_Call', 'strikePrice_Put','lastPrice_Put', 'impliedVolatility_Put','changeinOpenInterest_Put', 'totalTradedVolume_Put','openInterest_Put'  ])

    dfputsatm['%OI_Put'] = round(
        (dfputsatm.changeinOpenInterest_Put / (dfputsatm.openInterest_Put - dfputsatm.changeinOpenInterest_Put)) * 100, 2)
    dfputsatm['CHOI_Vol_Put'] = round((dfputsatm.openInterest_Put / dfputsatm.totalTradedVolume_Put), 2)

    dfputsatm['%OI_Call'] = round(
        (dfputsatm.changeinOpenInterest_Call / (dfputsatm.openInterest_Call - dfputsatm.changeinOpenInterest_Call)) * 100, 2)
    dfputsatm['CHOI_Vol_Call'] = round((dfputsatm.openInterest_Call / dfputsatm.totalTradedVolume_Call), 2)
    dfputsatm['PCR'] = round((dfputsatm.openInterest_Put / dfputsatm.openInterest_Call), 2)
    dfputsatm = dfputsatm.filter(
        ['openInterest_Call', 'totalTradedVolume_Call', 'changeinOpenInterest_Call', 'impliedVolatility_Call', 'lastPrice_Call', '%OI_Call', 'CHOI_Vol_Call','strikePrice_Put',
         'PCR','%OI_Put','CHOI_Vol_Put','lastPrice_Put',
         'impliedVolatility_Put', 'changeinOpenInterest_Put', 'totalTradedVolume_Put', 'openInterest_Put', ])
    dfcallsatm['%OI_Call'] = round((dfcallsatm.changeinOpenInterest_Call / (dfcallsatm.openInterest_Call - dfcallsatm.changeinOpenInterest_Call)) *100,2)
    dfcallsatm['CHOI_Vol_Call'] = round((dfcallsatm.openInterest_Call / dfcallsatm.totalTradedVolume_Call),2)
    dfcallsatm['%OI_Put'] = round(
        (dfcallsatm.changeinOpenInterest_Call / (dfcallsatm.openInterest_Put - dfcallsatm.changeinOpenInterest_Call)) * 100, 2)

    dfcallsatm['CHOI_Vol_Put'] = round((dfcallsatm.openInterest_Put / dfcallsatm.totalTradedVolume_Put), 2)

    dfcallsatm['PCR'] = round((dfcallsatm.openInterest_Put / dfcallsatm.openInterest_Call), 2)
    dfcallsatm = dfcallsatm.filter(
        ['openInterest_Call', 'totalTradedVolume_Call', 'changeinOpenInterest_Call', 'impliedVolatility_Call', 'lastPrice_Call', '%OI_Call', 'CHOI_Vol_Call','strikePrice_Call',
         'PCR','%OI_Put','CHOI_Vol_Put','lastPrice_Put',
         'impliedVolatility_Put', 'changeinOpenInterest_Put', 'totalTradedVolume_Put', 'openInterest_Put', ])



    dfcallsatm.reset_index(inplace=True,drop=True)
    dfputsatm.reset_index(inplace=True,drop=True)
    try:
        activecall,activeput = mostactive()
        activecorrput =activeput.corr()
        activecorrcall =activecall.corr()
        wb.sheets['Dashboard'].range("F5").options(index=False, header=False).value = activecall
        wb.sheets['Dashboard'].range("F8").options(index=False, header=False).value = activeput
        actcallvalspot = 'strong' if 0.80 <= activecorrcall['underlyingValue']['value'] <= 1.0 else 'positive' \
            if 0.50 <= activecorrcall['underlyingValue']['value'] < 0.80 else 'strongInv' \
            if -1.0 <= activecorrcall['underlyingValue']['value'] <= -0.80 else 'Inverse' \
            if -0.80 < activecorrcall['underlyingValue']['value'] <= -0.50 else 'WeakInv'

        actputvalspot = 'strong' if 0.80 <= activecorrput['underlyingValue']['value'] <= 1.0 else 'positive' \
            if 0.50 <= activecorrput['underlyingValue']['value'] < 0.80 else 'strongInv' \
            if -1.0 <= activecorrput['underlyingValue']['value'] <= -0.80 else 'Inverse' \
            if -0.80 < activecorrput['underlyingValue']['value'] <= -0.50 else 'WeakInv'
        wb.sheets['Dashboard'].range("O2").options(index=False, header=True).value = actcallvalspot
        wb.sheets['Dashboard'].range("O3").options(index=False, header=False).value = actputvalspot
    except Exception as error:
        print("Error in calling mostactive. Error : {0}".format(error))
    # Creating a data frame for ATM end here ######
    # #Probability calculation
    callDecay = df.nlargest(span, largestcall, keep='last')['lastPrice_Call'].astype(float)
    cdmean = round(df.nlargest(span, largestcall, keep='last')['lastPrice_Call'].astype(float).mean(),2)
    callputDecay = df.nlargest(span, largestcall, keep='last')['lastPrice_Put'].astype(float)
    callOI = df.nlargest(span, largestcall, keep='last')['openInterest_Call'].astype(float)
    callputOI = df.nlargest(span, largestcall, keep='last')['openInterest_Put'].astype(float)

    callvol = df.nlargest(span, largestcall, keep='last')['totalTradedVolume_Call'].astype(float)
    callputvol = df.nlargest(span, largestcall, keep='last')['totalTradedVolume_Put'].astype(float)

    callCHOI = df.nlargest(span, largestcall, keep='last')['changeinOpenInterest_Call'].astype(float)
    callputCHOI = df.nlargest(span,largestcall, keep='last')['changeinOpenInterest_Put'].astype(float)


    IVdatacall = df.nlargest(span, largestcall, keep='last')['impliedVolatility_Call'].astype(float)
    IVdatacallput = df.nlargest(span, largestcall, keep='last')['impliedVolatility_Put'].astype(float)

    callstrikepice = df.nlargest(span, largestcall, keep='last')['strikePrice_Call']

    putDecay = df.nlargest(span, largestput, keep='last')['lastPrice_Put'].astype(float)

    pdmean = round(df.nlargest(span, largestput, keep='last')['lastPrice_Put'].astype(float).mean(),2)

    putcallDecay = df.nlargest(span, largestput, keep='last')['lastPrice_Call'].astype(float)

    putOI = df.nlargest(span, largestput, keep='last')['openInterest_Put'].astype(float)
    putcallOI = df.nlargest(span, largestput, keep='last')['openInterest_Call'].astype(float)

    putvol = df.nlargest(span, largestput, keep='last')['totalTradedVolume_Put'].astype(float)
    putcallvol = df.nlargest(span, largestput, keep='last')['totalTradedVolume_Call'].astype(float)

    putCHOI = df.nlargest(span,largestput, keep='last')['changeinOpenInterest_Put'].astype(float)
    putcallCHOI = df.nlargest(span, largestput, keep='last')['changeinOpenInterest_Call'].astype(float)

    IVdataput = df.nlargest(span, largestput, keep='last')['impliedVolatility_Put'].astype(float)
    IVdataputcall = df.nlargest(span, largestput, keep='last')['impliedVolatility_Call'].astype(float)

    putstrikeprice =  df.nlargest(span, largestput, keep='last')['strikePrice_Put']

    decay = cdmean - pdmean

    wb.sheets['Dashboard'].range("F2").options(index=False, header=False).value = decay
    # Max pain calculation starts here

    df['sumOI'] = df['changeinOpenInterest_Call'] + df['changeinOpenInterest_Put']

    df['Underlying'] = nifty['lastPrice'].item()

    maxpain = df.nlargest(3, 'sumOI', keep='last')['strikePrice_Put']

    # Breaking first 5 strikes into individual columns end here

    df_call = pd.DataFrame(callOI)
    df_put = pd.DataFrame(putOI)
    df_call = df_call.join(callCHOI)
    df_call = df_call.join(callvol)
    df_call = df_call.join(callDecay)
    df_call = df_call.join(IVdatacall)
    df_call = df_call.join(callstrikepice)
    df_call = df_call.join(IVdatacallput)
    df_call = df_call.join(callputDecay)
    df_call = df_call.join(callputvol)
    df_call = df_call.join(callputCHOI)
    df_call = df_call.join(callputOI)

    df_put = df_put.join(putCHOI)
    df_put = df_put.join(putvol)
    df_put = df_put.join(putDecay)
    df_put = df_put.join(IVdataput)
    df_put = df_put.join(putstrikeprice)
    df_put = df_put.join(IVdataputcall)
    df_put = df_put.join(putcallDecay)
    df_put = df_put.join(putcallvol)
    df_put = df_put.join(putcallCHOI)
    df_put = df_put.join(putcallOI)

    df_call['%OI_Call'] = round((df_call.changeinOpenInterest_Call / (df_call.openInterest_Call - df_call.changeinOpenInterest_Call)) *100,2)
    df_call['CHOI_Vol_Call'] = round((df_call.openInterest_Call / df_call.totalTradedVolume_Call),2)

    df_call['%OI_Put'] = round(
        (df_call.openInterest_Call / (df_call.openInterest_Put - df_call.changeinOpenInterest_Put)) * 100, 2)


    df_call['CHOI_Vol_Put'] = round((df_call.openInterest_Put / df_call.totalTradedVolume_Put), 2)

    df_call['PCR'] = round((df_call.openInterest_Put / df_call.openInterest_Call), 2)


    df_put['%OI_Call'] = round((df_put.changeinOpenInterest_Call / (df_put.openInterest_Call - df_put.changeinOpenInterest_Call)) *100,2)
    df_put['CHOI_Vol_Call'] = round((df_put.changeinOpenInterest_Call/ df_put.totalTradedVolume_Call),2)

    df_put['%OI_Put'] = round(
        (df_put.changeinOpenInterest_Call / (df_put.openInterest_Put - df_put.changeinOpenInterest_Put)) * 100, 2)


    df_put['CHOI_Vol_Put'] = round((df_put.changeinOpenInterest_Put / df_put.totalTradedVolume_Put), 2)
    df_put['PCR'] = round((df_put.openInterest_Put / df_put.openInterest_Call), 2)

    df_call = pd.concat([df_call, dfcallsatm])
    df_call['Time'] = datetime.now().strftime("%d-%m %H:%M")
    df_cdata = pd.concat([df_cdata, df_call])

    df_put.reset_index(drop=True, inplace=True)

    df_put = pd.concat([df_put, dfputsatm])
    df_put['Time'] = datetime.now().strftime("%d-%m %H:%M")
    df_pdata = pd.concat([df_pdata, df_put])

    df_pdata.reset_index(drop=True, inplace=True)
    df_cdata.reset_index(drop=True, inplace=True)

    df_data = pd.concat([df_call, df_put])

    df_call.reset_index(drop=True, inplace=True)
    df_put.reset_index(drop=True, inplace=True)

    # df_data['callsOTM'] = (df_data.Strike_Price > round(float(niftyspot), 0))
    # df_data['putsOTM'] = (df_data.Strike_Price < round(float(niftyspot), 0))
    df_data.reset_index(drop=True, inplace=True)

    wb.sheets['CallData'].range("A1").options(index=False, header=True).value = df_cdata[df_cdata.columns[::-1]]
    wb.sheets['PutData'].range("A1").options(index=False, header=True).value = df_pdata[df_pdata.columns[::-1]]

    df_data['Underlying'] = nifty['lastPrice'].item()

    for i in df_call.index:
        df_data.loc[i, 'dfivcalprob'] = scipy.stats.norm.cdf(
            np.log( float(float(df_put.loc[i,'strikePrice_Put']) / niftyspot)) / ((float(df_call.loc[i,'impliedVolatility_Call'])/ 100) * np.sqrt(daystoexpiry / 365)))
        df_data.loc[i, 'ivputprob'] = 1 - scipy.stats.norm.cdf(
            np.log( float(float(df_call.loc[i,'strikePrice_Call']) / niftyspot)) / (( float(df_put.loc[i,'impliedVolatility_Put'] )/ 100) * np.sqrt(daystoexpiry / 365)))
        df_data.loc[i,'Direction'] = 'UP' if ((df_data.loc[i, 'dfivcalprob'] > df_data.loc[i, 'ivputprob']) & (cdmean > pdmean)) else 'DOWN' if ((df_data.loc[i, 'dfivcalprob'] <   df_data.loc[i, 'ivputprob']) &  (cdmean < pdmean) ) else '-'


    mp_dict_call = {datetime.now().strftime("%d-%m %H:%M"): {'underlying':nifty['lastPrice'].item(),
                                                             'MaxPain': maxpain.iloc[0],
                                                             'PCR':  dfcallsatm['PCR'],
                                                             'OI_Call': dfcallsatm['openInterest_Call'][0],
                                                             'Chng_in_OI_Call': dfcallsatm['changeinOpenInterest_Call'][0],
                                                             'Volume_Call': dfcallsatm['totalTradedVolume_Call'][0],
                                                             'Strikeprice': dfcallsatm['strikePrice_Call'][0],
                                                             'LTP_Call': dfcallsatm['lastPrice_Call'][0],
                                                             'IV_Call': dfcallsatm['impliedVolatility_Call'][0],
                                                             'OI_Put': dfcallsatm['openInterest_Put'][0],
                                                             'Chng_in_OI_Put': dfcallsatm['changeinOpenInterest_Put'][0],
                                                             'Volume_Put': dfcallsatm['totalTradedVolume_Put'][0],
                                                             'LTP_Put': dfcallsatm['lastPrice_Put'][0],
                                                             'IV_Put': dfcallsatm['impliedVolatility_Put'][0],
                                                             'avgltp': cdmean,
                                                             'decay': decay,
                                                             'vixweeklyhigh': wkhigh,
                                                             'vixweeklylow': wklow,
                                                             'vixdailyhigh': dailyhigh,
                                                             'vixdailylow': dailylow,
                                                             'vixspothigh': high,
                                                             'vixspotlow': low
                                                             }}

    mp_dict_put = {datetime.now().strftime("%d-%m %H:%M"): {'underlying': nifty['lastPrice'].item(),
                                                            'MaxPain': maxpain.iloc[0],
                                                            'PCR': dfputsatm['PCR'][0],
                                                            'OI_Call': dfputsatm['openInterest_Call'][0],
                                                            'Chng_in_OI_Call': dfputsatm['changeinOpenInterest_Call'][0],
                                                            'Volume_Call': dfputsatm['totalTradedVolume_Call'][0],
                                                            'Strikeprice': dfputsatm['strikePrice_Put'][0],
                                                            'LTP_Call': dfputsatm['lastPrice_Call'][0],
                                                            'IV_Call': dfputsatm['impliedVolatility_Call'][0],
                                                            'OI_Put': dfputsatm['openInterest_Put'][0],
                                                            'Chng_in_OI_Put': dfputsatm['changeinOpenInterest_Put'][0],
                                                            'Volume_Put': dfputsatm['totalTradedVolume_Put'][0],
                                                            'LTP_Put': dfputsatm['lastPrice_Put'][0],
                                                            'IV_Put': dfputsatm['impliedVolatility_Put'][0],
                                                            'avgltp': pdmean,
                                                            'decay': decay,
                                                            'vixweeklyhigh': wkhigh,
                                                            'vixweeklylow': wklow,
                                                            'vixdailyhigh': dailyhigh,
                                                            'vixdailylow': dailylow,
                                                            'vixspothigh':high,
                                                            'vixspotlow':low
                                                            }}

    dfcall4 = pd.DataFrame(mp_dict_call).transpose()
    dfput4 = pd.DataFrame(mp_dict_put).transpose()



    mpcall_df = pd.concat([mpcall_df, dfcall4], sort=False)
    mpput_df = pd.concat([mpput_df, dfput4], sort=False)

    mpcall_df.index.name = 'Time'
    mpput_df.index.name = 'Time'

    mpcall_df = mpcall_df.astype(float)
    mpput_df = mpput_df.astype(float)

    dfcall_corr = mpcall_df[::-1].filter(['OI_Call', 'Volume_Call', 'IV_Call', 'LTP_Call', 'underlying',
                                          'LTP_Put', 'IV_Put', 'Volume_Put', 'OI_Put'])

    dfput_corr = mpput_df[::-1].filter(
        ['OI_Call', 'Volume_Call', 'IV_Call', 'LTP_Call', 'underlying',
         'LTP_Put', 'IV_Put', 'Volume_Put', 'OI_Put'])

    # Correlation of top 5 rows call and puts

    dfcall_corr = dfcall_corr.head(5).corr()[
        ['OI_Call', 'Volume_Call', 'IV_Call', 'LTP_Call', 'underlying']]
    dfcall_corr = dfcall_corr.transpose()

    dfput_corr = dfput_corr.head(5).corr()[['OI_Put', 'Volume_Put', 'LTP_Put', 'IV_Put', 'underlying']]
    dfput_corr = dfput_corr.transpose()

    # End  Correlation of top 5 rows call and puts




    wb.sheets['Dashboard'].range("A32").options(index=False, header=False).value = df_data[['openInterest_Call',
                                                                                            'changeinOpenInterest_Call',
                                                                                            'totalTradedVolume_Call',
                                                                                            'lastPrice_Call',
                                                                                            'impliedVolatility_Call',
                                                                                            '%OI_Call',
                                                                                            'CHOI_Vol_Call','dfivcalprob','Direction',
                                                                                            'strikePrice_Call','ivputprob',
                                                                                            'CHOI_Vol_Put','%OI_Put','impliedVolatility_Put','lastPrice_Put','totalTradedVolume_Put','changeinOpenInterest_Put','openInterest_Put','PCR']]
    wb.sheets['Dashboard'].range("J36").options(index=False, header=False).value = df_data[['strikePrice_Put']].dropna()
    wb.sheets['Dashboard'].range("Q1").options(index=False, header=False).value = maxpain
    wb.sheets['Dashboard'].range("J2").options(index=False, header=False).value = datetime.now().strftime(
        "%H:%M")
    wb.sheets['Dashboard'].range("J1").options(index=False, header=False).value = niftyspot
    wb.sheets['Dashboard'].range("B2").options(index=False, header=False).value = 'Daily'
    wb.sheets['Dashboard'].range("C2").options(index=False, header=False).value = dailyhigh
    wb.sheets['Dashboard'].range("D2").options(index=False, header=False).value = dailylow
    wb.sheets['Dashboard'].range("B3").options(index=False, header=False).value = 'Weekly'
    wb.sheets['Dashboard'].range("C3").options(index=False, header=False).value = wkhigh
    wb.sheets['Dashboard'].range("D3").options(index=False, header=False).value = wklow
    wb.sheets['Dashboard'].range("S2").options(index=False, header=False).value = high
    wb.sheets['Dashboard'].range("T2").options(index=False, header=False).value = low
    df_data['Time'] = datetime.now().strftime("%d-%m %H:%M")

    # correlation call side

    corrivvolcall = 'strong' if 0.80 <= dfcall_corr['IV_Call']['Volume_Call'] <= 1.0 else 'positive' \
        if 0.50 <= dfcall_corr['IV_Call']['Volume_Call'] < 0.80 else 'strongInv' \
        if -1.0 <= dfcall_corr['IV_Call']['Volume_Call'] <= -0.80 else 'Inverse' \
        if -0.80 < dfcall_corr['IV_Call']['Volume_Call'] <= -0.50 else 'WeakInv'

    corrivniftycall = 'strong' if 0.80 <= dfcall_corr['IV_Call']['underlying'] <= 1.0 else 'positive' \
        if 0.50 <= dfcall_corr['IV_Call']['underlying'] < 0.80 else 'strongInv' \
        if -1.0 <= dfcall_corr['IV_Call']['underlying'] <= -0.80 else 'Inverse' \
        if -0.80 < dfcall_corr['IV_Call']['underlying'] <= -0.50 else 'WeakInv'

    corrivltpcall = 'strong' if 0.80 <= dfcall_corr['IV_Call']['LTP_Call'] <= 1.0 else 'positive' \
        if 0.50 <= dfcall_corr['IV_Call']['LTP_Call'] < 0.80 else 'strongInv' \
        if -1.0 <= dfcall_corr['IV_Call']['LTP_Call'] <= -0.80 else 'Inverse' \
        if -0.80 < dfcall_corr['IV_Call']['LTP_Call'] <= -0.50 else 'Weak'

    # correlation for put side

    corrivvolput = 'strong' if 0.80 <= dfput_corr['IV_Put']['Volume_Put'] <= 1.0 else 'positive' \
        if 0.50 <= dfput_corr['IV_Put']['Volume_Put'] < 0.80 else 'strongInv' \
        if -1.0 <= dfput_corr['IV_Put']['Volume_Put'] <= -0.80 else 'Inverse' \
        if -0.80 < dfput_corr['IV_Put']['Volume_Put'] <= -0.50 else 'Weak'

    corrivniftyput = 'strong' if 0.80 <= dfput_corr['IV_Put']['underlying'] <= 1.0 else 'positive' \
        if 0.50 <= dfput_corr['IV_Put']['underlying'] < 0.80 else 'strongInv' \
        if -1.0 <= dfput_corr['IV_Put']['underlying'] <= -0.80 else 'Inverse' \
        if -0.80 < dfput_corr['IV_Put']['underlying'] <= -0.50 else 'Weak'

    corrivltpput = 'strong' if 0.80 <= dfput_corr['IV_Put']['LTP_Put'] <= 1.0 else 'positive' \
        if 0.50 <= dfput_corr['IV_Put']['LTP_Put'] < 0.80 else 'strongInv' \
        if -1.0 <= dfput_corr['IV_Put']['LTP_Put'] <= -0.80 else 'Inverse' \
        if -0.80 < dfput_corr['IV_Put']['underlying'] <= -0.50 else 'Weak'

    wb.sheets['Dashboard'].range("L3").options(index=False, header=False).value = corrivniftyput
    wb.sheets['Dashboard'].range("M3").options(index=False, header=False).value = corrivvolput
    wb.sheets['Dashboard'].range("N3").options(index=False, header=False).value = corrivltpput

    wb.sheets['Dashboard'].range("L2").options(index=False, header=False).value = corrivniftycall
    wb.sheets['Dashboard'].range("M2").options(index=False, header=False).value = corrivvolcall
    wb.sheets['Dashboard'].range("N2").options(index=False, header=False).value = corrivltpcall

    #wb.sheets['Dashboard'].range("A34").options(index=True, header=False).value = dfcall_corr
    #wb.sheets['Dashboard'].range("L34").options(index=True, header=False).value = dfput_corr
    return df, mpcall_df,mpput_df, df_cdata, df_pdata, df_diff

def main():
    global df_list, dffut
    global mpdict_activecall
    global mpdict_activecput
    waitsecs =0
    counter = 0
 #   get_session_cookies()
    niftyspotfun()
    mostactive()
    df_cdata = pd.DataFrame()
    df_pdata = pd.DataFrame()
    df_diff = pd.DataFrame()
    dffut= pd.DataFrame()
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


#######Futurest Data##############
    try:
        fut_list = json.loads(open(fut_filename).read())
        print('fut_list ', fut_list)
        df_fut = pd.DataFrame().from_dict(fut_list)

    except Exception as error:
        print("Error reading data. Error : {0}".format(error))
        fut_list = []
        df_fut = pd.DataFrame()

################

    try:

        mpcall_list = json.loads(open(mpcall_filename).read())
        # mp_list = ast.literal_eval(mp_list)
        mpcall_df = pd.DataFrame().from_dict(mpcall_list)


        mpput_list = json.loads(open(mpput_filename).read())
        # mp_list = ast.literal_eval(mp_list)
        mpput_df = pd.DataFrame().from_dict(mpput_list)

    except Exception as error:
        print("Error reading data. Error : {0}".format(error))
        mpcall_df = pd.DataFrame()
        mpput_df = pd.DataFrame()


    if not (start <= datetime.now().time() <= end):
        print("Market is off as of now, please run during market hours")
        quit()

    while start <= datetime.now().time() <= end:
        try:

            timenow = datetime.now()
            check = True if round(int(str(timenow.minute)) / timeframe) in list(np.arange(0.00, interval)) else False
            print(round(int(str(timenow.minute)) / timeframe))
            print(round(int(str(timenow.minute)) / timeframe))
            print(check)

            if check:

                print('waitsecs',waitsecs)
                nextscan = timenow + timedelta(minutes=timeframe)

                waitsecs = int((nextscan - datetime.now()).seconds)

                ########################Furture data collection############
                print('before get future data')
                df_fut = getlivefuture(df_fut)

                print('Future Data has been read..')

                wb.sheets['FutureData'].range("A1").options(header=True).value = df_fut
                print('Future Data written to sheet')
               # print('before writing future date')
                try:
                    with open(fut_filename, "w") as files:
                       files.write(json.dumps(df_fut.to_dict(), cls=NpEncoder, indent=4, sort_keys=True))
                except Exception as error:
                    print("Error in main. Error fut_filename: {0}".format(error))
                    continue
                ###################### Furture data collection ends here###########
                print('Future data')
                ########################Option Chain data collection############

                df, mpcall_df,mpput_df, df_cdata, df_pdata,  df_diff = \
                    getoptionchain(df, mpcall_df,mpput_df, df_cdata, df_pdata, df_diff)

                #####Calling zcoremain to project data regarding atm prices
                print( "Calling zscore main")
                zscoremain()
                print("processing ended of zscore main")


                #####Calling zcoremain to project data regarding atm prices
                wb.sheets['MPDatacall'].range("A1").options(header=True).value = mpcall_df
                wb.sheets['MPDataput'].range("A1").options(header=True).value = mpput_df
                ###recording strikepricewise changeing data###

                wb.sheets['MPDatacall'].range("A1").options(header=True).value = mpcall_df
                wb.sheets['MPDataput'].range("A1").options(header=True).value = mpput_df

                ###recording strikepricewise changeing data###

                with open(mpcall_filename, "w") as files:
                    files.write(json.dumps(mpcall_df.to_dict(), cls=NpEncoder, indent=4, sort_keys=True))
                    # wb.api.RefreshAll() with open(mp_filename, "w") as files:

                with open(mpput_filename, "w") as files:
                    files.write(json.dumps(mpput_df.to_dict(), cls=NpEncoder, indent=4, sort_keys=True))
                    # wb.api.RefreshAll()

                df_list.append(df.to_dict('records'))

                with open(oi_filename, "w") as files:
                    files.write(json.dumps(df_list, indent=4, sort_keys=True))

                ###################### Option Chain data collection ends here###########

            wb.save()

            if not df.empty:


                df['impliedVolatility_Call'] = df['impliedVolatility_Call'].replace(to_replace=0, method='bfill').values
                df['impliedVolatility_Put'] = df['impliedVolatility_Put'].replace(to_replace=0, method='bfill').values
                df['datetime'] = datetime.now().strftime("%d-%m-%y%y %H:%M")

                sht_live.range("A2").options(header=True, index=False).value = df

                if (startref <= datetime.now().time() <= endref ):
                    print("Reference values have been collected, closing the program")
                    quit()

                # wb.api.RefreshAll()
                nextscan = timenow + timedelta(minutes=timeframe)
                waitsecs = int((nextscan - datetime.now()).seconds)
                print("Wait for {0} seconds".format(waitsecs))

                sleep(waitsecs) if waitsecs > 0 else sleep(0)

            else:
                print("No data received")



        except Exception as error:
            print("Error in main. Error : {0}".format(error))
            sleep(waitsecs)
            continue


if __name__ == '__main__':
    main()
