from scipy.stats import norm as stats
from utility import *
from NseDataFetch import *
from datetime import datetime, timedelta, time
import numpy as np
import json
import configparser
from sklearn.preprocessing import StandardScaler
import warnings

# path = config.get('filedetailswip', 'filelocationwip')
# filename = config.get('filedetailswip', 'NseIndexlivewip.filename')

path = config.get('filedetails', 'filelocation')
filename = config.get('filedetails', 'NseIndexlive.filename')
excel_file = path + filename
wb = xw.Book(excel_file)

warnings.simplefilter(action="ignore")

total_weight= .3 + .15 +.25 +.3
CHOI_Vol_weight = .3 /total_weight
CHOI_weight = .15 /total_weight
IV_weight = .25 /total_weight
premiumdecay_weight = .3 /total_weight

t_factor = 2
t_multiple = 1

pd.set_option('display.width', 1500)
pd.set_option('display.max_columns', 75)
pd.set_option('display.max_rows', 1500)

dfreference = pd.DataFrame()

nearatmcall_filename = os.path.join(path + "/Intraday/Daily",
                                    "nearatmcalllive_{0}".format(datetime.now().strftime("%d%m%y")))
nearatmput_filename = os.path.join(path + "/Intraday/Daily",
                                   "nearatmputlive_{0}".format(datetime.now().strftime("%d%m%y")))
zscore_filename = os.path.join(path + "/Intraday/Daily",
                               "zscorelive_{0}".format(datetime.now().strftime("%d%m%y")))


# nearatmcall_filename = os.path.join(path + "/Intraday/WIP",
#                                     "nearatmcalllive_{0}".format(datetime.now().strftime("%d%m%y")))
# nearatmput_filename = os.path.join(path + "/Intraday/WIP",
#                                    "nearatmputlive_{0}".format(datetime.now().strftime("%d%m%y")))
# zscore_filename = os.path.join(path + "/Intraday/WIP",
#                                "zscorelive_{0}".format(datetime.now().strftime("%d%m%y")))


def z_score(df):
    # copy the dataframe
    p_values = pd.DataFrame()

    df_std = df.copy()
    # apply the z-score method

    for column in df_std.columns:
        df_std[column] = (df_std[column] - df_std[column].mean()) / df_std[column].std()

        # if column == 'CHOI_Vol_Call' or column == 'CHOI_Vol_Put':
        #     df_std[column] = df_std[column] * CHOI_Vol_weight
        #
        # if column == 'openInterest_Call' or column == 'openInterest_Put':
        #     df_std[column] = df_std[column] * CHOI_weight
        #
        # if column == 'lastPrice_Call' or column == 'lastPrice_Put':
        #     df_std[column] = df_std[column] * premiumdecay_weight
        #
        # if column == 'impliedVolatility_Call' or column == 'impliedVolatility_Put':
        #     df_std[column] = df_std[column] * IV_weight

    df_std = t_factor - (df_std * t_multiple)

    return round(df_std, 2)


def nearatmstrikes(mpcall_df, df_opdata, dfzscorerec):
    df, ce_data, pe_data = niftyoptionchain()
    nifty = niftyspotfun()
    niftyspot = nifty['lastPrice'].values
    ce_data['callsOTM'] = ce_data.strikePrice >= roundx(niftyspot)
    pe_data['putsOTM'] = pe_data.strikePrice <= roundx(niftyspot)
    print('processing near atm strikes data.......')
    df_callhead = (ce_data[ce_data['callsOTM'] == True]).head(2)
    df_calltail = (ce_data[ce_data['callsOTM'] == False]).tail(2)

    df_puthead = (pe_data[pe_data['putsOTM'] == False]).head(2)
    df_puttail = (pe_data[pe_data['putsOTM'] == True]).tail(2)
    df_put = pd.concat([df_puthead, df_puttail])
    df_call = pd.concat([df_callhead, df_calltail])

    df_put.reset_index(inplace=True, drop=True)
    df_call.reset_index(inplace=True, drop=True)

    df_opdata = pd.merge(df_call, df_put, how='inner', on='strikePrice')

    df_opdata.rename(columns={'underlyingValue_x': 'underlying', 'openInterest_x': 'openInterest_Call',
                              'changeinOpenInterest_x': 'changeinOpenInterest_Call',
                              'totalTradedVolume_x': 'totalTradedVolume_Call',
                              'impliedVolatility_x': 'impliedVolatility_Call', 'lastPrice_x': 'lastPrice_Call',
                              'openInterest_y': 'openInterest_Put',
                              'changeinOpenInterest_y': 'changeinOpenInterest_Put',
                              'totalTradedVolume_y': 'totalTradedVolume_Put',
                              'impliedVolatility_y': 'impliedVolatility_Put', 'lastPrice_y': 'lastPrice_Put'
                              }, inplace=True)

    print('dataframe df_opdata')
    df_opdata = df_opdata.filter(
        ['Time', 'underlying', 'openInterest_Call', 'totalTradedVolume_Call', 'changeinOpenInterest_Call',
         'impliedVolatility_Call',
         'lastPrice_Call', 'strikePrice', 'lastPrice_Put', 'impliedVolatility_Put', 'changeinOpenInterest_Put',
         'totalTradedVolume_Put', 'openInterest_Put'])

    df_opdata['CHOI_Vol_Put'] = round((df_opdata.openInterest_Put / df_opdata.totalTradedVolume_Put), 2)
    df_opdata['CHOI_Vol_Call'] = round((df_opdata.openInterest_Call / df_opdata.totalTradedVolume_Call), 2)

    df['Underlying'] = nifty['lastPrice'].item()
    print('###############################df_call & df_put################################### ')

    print('###############################Ends here df_call################################### ')

    df_opdata.reset_index(drop=True, inplace=True)
    print('Printing opdata and cdata')
    print(df_opdata)

    print('###############################Ends here df_call################################### ')

    dfopscore = z_score(df_opdata)
    print('Printing z score')

    callscorematrix = pd.DataFrame()
    putscorematrix = pd.DataFrame()
    maxmatrix = pd.DataFrame()

    maxmatrix['CallWriter'] = 0
    maxmatrix['PutWriter'] = 0
    callscorematrix['CHOI_Vol_Callweight'] = 0
    callscorematrix['lastPrice_Callweight'] = 0
    callscorematrix['impliedVolatility_Callweight'] = 0
    callscorematrix['changeinOpenInterest_Callweight'] = 0
    callscorematrix['OpenInterest_Callweight'] = 1
    callscorematrix['totalTradedVolume_Callweight'] = 1

    putscorematrix['CHOI_Vol_Putweight'] = 0
    putscorematrix['lastPrice_Putweight'] = 0
    putscorematrix['impliedVolatility_Putweight'] = 0
    putscorematrix['changeinOpenInterest_Putweight'] = 0
    putscorematrix['OpenInterest_Putweight'] = 1
    putscorematrix['totalTradedVolume_Putweight'] = 1

    #########Assigning weights#######################

    callscorematrix['CHOI_Vol_Callweight'] = dfopscore['CHOI_Vol_Call'] * CHOI_Vol_weight
    putscorematrix['CHOI_Vol_Putweight'] = dfopscore['CHOI_Vol_Put'] * CHOI_Vol_weight

    callscorematrix['changeinOpenInterest_Callweight'] = dfopscore['changeinOpenInterest_Call'] * CHOI_weight
    putscorematrix['changeinOpenInterest_Putweight'] = dfopscore['changeinOpenInterest_Put'] * CHOI_weight

    callscorematrix['impliedVolatility_Callweight'] = dfopscore['impliedVolatility_Call'] * IV_weight
    putscorematrix['impliedVolatility_Putweight'] = dfopscore['impliedVolatility_Put'] * IV_weight

    callscorematrix['lastPrice_Callweight'] = dfopscore['lastPrice_Call'] * premiumdecay_weight
    putscorematrix['lastPrice_Putweight'] = dfopscore['lastPrice_Put'] * premiumdecay_weight

    callscorematrix['OpenInterest_Callweight'] = dfopscore['openInterest_Call']
    putscorematrix['OpenInterest_Putweight'] = dfopscore['openInterest_Put']

    callscorematrix['OpenInterest_Callweight'] = dfopscore['totalTradedVolume_Call']
    putscorematrix['OpenInterest_Putweight'] = dfopscore['totalTradedVolume_Put']

    #########Assigning weights Ends #######################

    ####TOPSIS Analysis implementation starts here##########################################
    ####Video link referred : https://www.youtube.com/watch?v=kJuS6UVzkHo&list=WL&index=2###########

    ######Need to take minimuum value hence addiding put to call writer and vice versa#########

    maxmatrixcall = pd.DataFrame(callscorematrix.max(axis=0)).transpose()
    minmatrixcall = pd.DataFrame(callscorematrix.min(axis=0)).transpose()

    maxmatrixput = pd.DataFrame(putscorematrix.max(axis=0)).transpose()
    minmatrixput = pd.DataFrame(putscorematrix.min(axis=0)).transpose()

    callwriterspositive = maxmatrixcall[
        ['lastPrice_Callweight', 'impliedVolatility_Callweight', 'changeinOpenInterest_Callweight',
         'OpenInterest_Callweight', 'totalTradedVolume_Callweight']]
    callwriterspositive['CHOI_Vol_Callweight'] = minmatrixcall[['CHOI_Vol_Callweight']]

    callwriterspositive = callwriterspositive[
        ['CHOI_Vol_Callweight', 'lastPrice_Callweight', 'impliedVolatility_Callweight',
         'changeinOpenInterest_Callweight', 'OpenInterest_Callweight', 'totalTradedVolume_Callweight']]

    callwritersnegative = minmatrixcall[
        ['lastPrice_Callweight', 'impliedVolatility_Callweight', 'changeinOpenInterest_Callweight',
         'OpenInterest_Callweight', 'totalTradedVolume_Callweight']]
    callwritersnegative['CHOI_Vol_Callweight'] = maxmatrixcall[['CHOI_Vol_Callweight']]
    #  callwritersnegative =callwritersnegative[['CHOI_Vol_Callweight','lastPrice_Callweight', 'impliedVolatility_Callweight', 'changeinOpenInterest_Callweight','OpenInterest_Callweight','totalTradedVolume_Callweight']]

    putwriterspositive = maxmatrixput[
        ['lastPrice_Putweight', 'impliedVolatility_Putweight', 'changeinOpenInterest_Putweight',
         'OpenInterest_Putweight', 'totalTradedVolume_Putweight']]
    putwriterspositive['CHOI_Vol_Putweight'] = minmatrixput[['CHOI_Vol_Putweight']]

    #  putwriterspositive = putwriterspositive[['CHOI_Vol_Putweight','lastPrice_Putweight', 'impliedVolatility_Putweight', 'changeinOpenInterest_Putweight','OpenInterest_Putweight','totalTradedVolume_Putweight']]

    putwritersnegative = minmatrixput[
        ['lastPrice_Putweight', 'impliedVolatility_Putweight', 'changeinOpenInterest_Putweight',
         'OpenInterest_Putweight', 'totalTradedVolume_Putweight']]
    putwritersnegative['CHOI_Vol_Putweight'] = maxmatrixput[['CHOI_Vol_Putweight']]
    putwritersnegative = putwritersnegative[
        ['CHOI_Vol_Putweight', 'lastPrice_Putweight', 'impliedVolatility_Putweight', 'changeinOpenInterest_Putweight',
         'OpenInterest_Putweight', 'totalTradedVolume_Putweight']]

    callscorematrix.reset_index(inplace=True, drop=True)
    putscorematrix.reset_index(inplace=True, drop=True)

    first_rowcallpos = callwriterspositive.iloc[[0]].values[0]
    first_rowcallneg = putwritersnegative.iloc[[0]].values[0]
    calldistancecallpos = callscorematrix.apply(lambda row: row - first_rowcallpos, axis=1)
    calldistancecallpos = round(np.square(calldistancecallpos), 2)
    calldistancecallpos['total'] = calldistancecallpos.sum(axis=1)
    calldistancecallpos['sqrt'] = round(np.sqrt(calldistancecallpos['total']), 2)

    calldistancecallneg = callscorematrix.apply(lambda row: row - first_rowcallneg, axis=1)
    calldistancecallneg = round(np.square(calldistancecallneg), 2)
    calldistancecallneg['total'] = calldistancecallneg.sum(axis=1)
    calldistancecallneg['sqrt'] = round(np.sqrt(calldistancecallneg['total']), 2)

    first_rowputpos = callwriterspositive.iloc[[0]].values[0]

    first_rowputneg = putwritersnegative.iloc[[0]].values[0]

    print('Printing matrixes')
    print(first_rowputpos)
    print(first_rowputneg)
    putdistanceputpos = putscorematrix.apply(lambda row: row - first_rowputpos, axis=1)
    putdistanceputneg = putscorematrix.apply(lambda row: row - first_rowputneg, axis=1)
    putdistanceputpos = round(np.square(putdistanceputpos), 2)

    # print(putdistanceputneg)
    putdistanceputpos['total'] = putdistanceputpos.sum(axis=1)
    putdistanceputpos['sqrt'] = round(np.sqrt(putdistanceputpos['total']), 2)
    putdistanceputneg = round(np.square(putdistanceputneg), 2)
    putdistanceputneg['total'] = putdistanceputneg.sum(axis=1)
    putdistanceputneg['sqrt'] = round(np.sqrt(putdistanceputneg['total']), 2)

    ###### Finding relative closeness formula in TOPSIS method#######################

    putdistanceputpos['closeness'] = round(
        (putdistanceputpos['sqrt'] / (putdistanceputneg['sqrt'] + putdistanceputpos['sqrt'])), 2)
    calldistancecallpos['closeness'] = round(
        (calldistancecallpos['sqrt'] / (calldistancecallpos['sqrt'] + calldistancecallneg['sqrt'])), 2)

    print('Printing data frames')
    putdistancemean = pd.DataFrame(putdistanceputpos.mean(axis=0)).transpose()
    calldistancemean = pd.DataFrame(calldistancecallpos.mean(axis=0)).transpose()
    print(dfzscorerec)
    print(putdistancemean)
    print(calldistancemean)

    dfopscoreatm = positions(dfreference, dfzscorerec, calldistancemean, putdistancemean)
    print('Print dfopscoreatm')
    print(dfopscoreatm)

    dfopscoreatm['call_score'] = (calldistancecallpos['closeness'].max()) * 100
    dfopscoreatm['put_score'] = (putdistanceputpos['closeness'].max()) * 100
    dfopscoreatm['score'] = round((dfopscoreatm['call_score'] - dfopscoreatm['put_score']), 2)

    df_opdata['Time'] = datetime.now().strftime("%d-%m %H:%M")

    wb.sheets['CallDataATM'].range("A1").options(index=False, header=True).value = df_opdata[df_opdata.columns[::-1]]
    print('###############################Ends here###################################')

    print('###############convertiing dataframe to dictionary Starts here################')

    zscore_dict = {datetime.now().strftime("%d-%m %H:%M"): {'underlying': nifty['lastPrice'].item(),
                                                            'OI_Call': dfopscoreatm['openInterest_Call'][0],
                                                            'Chng_in_OI_Call':
                                                                dfopscoreatm['changeinOpenInterest_Call'][
                                                                    0],
                                                            'Volume_Call': dfopscoreatm['totalTradedVolume_Call'][0],
                                                            'LTP_Call': dfopscoreatm['lastPrice_Call'][0],
                                                            'IV_Call': dfopscoreatm['impliedVolatility_Call'][0],
                                                            'CHOI_Vol_Call': dfopscoreatm['CHOI_Vol_Call'][0],
                                                            # 'Strikeprice': dfopscoreatm['strikePrice'][0],
                                                            'CHOI_Vol_Put': dfopscoreatm['CHOI_Vol_Put'][0],
                                                            'OI_Put': dfopscoreatm['openInterest_Put'][0],
                                                            'Chng_in_OI_Put': dfopscoreatm['changeinOpenInterest_Put'][
                                                                0],
                                                            'Volume_Put': dfopscoreatm['totalTradedVolume_Put'][0],
                                                            'LTP_Put': dfopscoreatm['lastPrice_Put'][0],
                                                            'IV_Put': dfopscoreatm['impliedVolatility_Put'][0],
                                                            'PutWritersActive': dfopscoreatm['PutWritersActive'][0],
                                                            'CallWritersActive': dfopscoreatm['CallWritersActive'][0],
                                                            'PutBuyersActive': dfopscoreatm['PutBuyersActive'][0],
                                                            'CallBuyersActive': dfopscoreatm['CallBuyersActive'][0],
                                                            'PutPremiumDecreasing':
                                                                dfopscoreatm['PutPremiumDecreasing'][0],
                                                            'CallPremiumDecreasing':
                                                                dfopscoreatm['CallPremiumDecreasing'][0],
                                                            'PutPremiumIncreasing':
                                                                dfopscoreatm['PutPremiumIncreasing'][0],
                                                            'CallPremiumIncreasing':
                                                                dfopscoreatm['CallPremiumIncreasing'][0],
                                                            #  'Close'             :dfopscoreatm['Close'],
                                                            #   'Fresh'             :dfopscoreatm['Fresh'],
                                                            'call_score': dfopscoreatm['call_score'][0],
                                                            'put_score': dfopscoreatm['put_score'][0],
                                                            'score': dfopscoreatm['score'][0]
                                                            }}

    mp_dict_opdata = {datetime.now().strftime("%d-%m %H:%M"): {'underlying': df_opdata['underlying'][0],
                                                               'OI_Call': df_opdata['openInterest_Call'][0],
                                                               'Chng_in_OI_Call':
                                                                   df_opdata['changeinOpenInterest_Call'][
                                                                       0],
                                                               'Volume_Call': df_opdata['totalTradedVolume_Call'][0],
                                                               'Strikeprice': df_opdata['strikePrice'][0],
                                                               'LTP_Call': df_opdata['lastPrice_Call'][0],
                                                               'IV_Call': df_opdata['impliedVolatility_Call'][0],
                                                               'OI_Put': df_opdata['openInterest_Put'][0],
                                                               'Chng_in_OI_Put': df_opdata['changeinOpenInterest_Put'][
                                                                   0],
                                                               'Volume_Put': df_opdata['totalTradedVolume_Put'][0],
                                                               'LTP_Put': df_opdata['lastPrice_Put'][0],
                                                               'IV_Put': df_opdata['impliedVolatility_Put'][0]
                                                               }}

    print('###############convertiing dataframe to dictionary ENDS here################')
    dfcall4 = pd.DataFrame(mp_dict_opdata).transpose()
    dfzscore = pd.DataFrame(zscore_dict).transpose()

    dfzscorerec = pd.concat([dfzscorerec, dfzscore], sort=False)
    mpcall_df = pd.concat([mpcall_df, dfcall4], sort=False)

    mpcall_df.index.name = 'Time'

    mpcall_df = mpcall_df.astype(float)

    wb.sheets['Dashboard'].range("A25").options(index=False, header=False).value = df_opdata[['openInterest_Call',
                                                                                              'changeinOpenInterest_Call',
                                                                                              'totalTradedVolume_Call',
                                                                                              'lastPrice_Call',
                                                                                              'impliedVolatility_Call',
                                                                                              'CHOI_Vol_Call',
                                                                                              'strikePrice',
                                                                                              'CHOI_Vol_Put',
                                                                                              'impliedVolatility_Put',
                                                                                              'lastPrice_Put',
                                                                                              'totalTradedVolume_Put',
                                                                                              'changeinOpenInterest_Put',
                                                                                              'openInterest_Put']]

    return mpcall_df, df_opdata, dfzscorerec


def positions(dfreference, dfzscorerec, calldistancemean, putdistancemean):
    dfreference = dfzscorerec.head(1)
    dfopscore = calldistancemean.join(putdistancemean, lsuffix='Call', rsuffix='Put')

    dfopscore.rename(columns={'CHOI_Vol_Callweight': 'CHOI_Vol_Call', 'lastPrice_Callweight': 'lastPrice_Call',
                              'impliedVolatility_Callweight': 'impliedVolatility_Call',
                              'changeinOpenInterest_Callweight': 'changeinOpenInterest_Call',
                              'OpenInterest_Callweight': 'openInterest_Call',
                              'totalTradedVolume_Callweight': 'totalTradedVolume_Call',
                              'CHOI_Vol_Putweight': 'CHOI_Vol_Put', 'lastPrice_Putweight': 'lastPrice_Put',
                              'impliedVolatility_Putweight': 'impliedVolatility_Put',
                              'changeinOpenInterest_Putweight': 'changeinOpenInterest_Put',
                              'OpenInterest_Putweight': 'openInterest_Put',
                              'totalTradedVolume_Putweight': 'totalTradedVolume_Put'}, inplace=True)

    dfopscoreatm = dfopscore.filter(
        ['CHOI_Vol_Call', 'lastPrice_Call', 'impliedVolatility_Call', 'totalTradedVolume_Call',
         'changeinOpenInterest_Call', 'openInterest_Call', 'CHOI_Vol_Put', 'impliedVolatility_Put',
         'totalTradedVolume_Put', 'changeinOpenInterest_Put', 'openInterest_Put', 'lastPrice_Put'])

    if dfreference.empty:
        dfreference = dfopscore.head(1)
        dfreference.rename(columns={'openInterest_Call': 'OI_Call', 'openInterest_Put': 'OI_Put',
                                    'totalTradedVolume_Call': 'Volume_Call',
                                    'totalTradedVolume_Put': 'Volume_Put',
                                    'lastPrice_Call': 'LTP_Call',
                                    'lastPrice_Put': 'LTP_Put',
                                    'impliedVolatility_Call': 'IV_Call',
                                    'impliedVolatility_Put': 'IV_Put'}, inplace=True)

    dfopscoreatm['CallBuyersActive'] = 'False'
    dfopscoreatm['CallWritersActive'] = 'False'
    dfopscoreatm['PutBuyersActive'] = 'False'
    dfopscoreatm['PutWritersActive'] = 'False'
    # dfopscoreatm['Fresh'] =  'FalseFresh'
    dfopscoreatm['CallPremiumIncreasing'] = 'False'
    dfopscoreatm['PutPremiumIncreasing'] = 'False'
    dfopscoreatm['CallPremiumDecreasing'] = 'False'
    dfopscoreatm['PutPremiumDecreasing'] = 'False'
    #  dfopscoreatm['Close'] =  'FalseClose'

    ###########Fresh positions Buyers active#################
    dfreference.reset_index(drop=True, inplace=True)
    dfopscoreatm.reset_index(drop=True, inplace=True)

    dfreference['changeinOpenInterest_Call'] = dfreference['OI_Call'] - dfopscoreatm['openInterest_Call']
    dfreference['changeinOpenInterest_Put'] = dfreference['OI_Put'] - dfopscoreatm['openInterest_Put']

    ##################Decay Calculations###########################

    if (dfreference['LTP_Call'] >= dfopscoreatm['lastPrice_Call']).all():
        if (dfopscoreatm['lastPrice_Call'] <= dfopscoreatm['lastPrice_Put']).all():
            dfopscoreatm['CallPremiumDecreasing'] = 'True'
        else:
            dfopscoreatm['PutPremiumDecreasing'] = 'True'
    else:
        if (dfopscoreatm['lastPrice_Call'] >= dfopscoreatm['lastPrice_Put']).all():
            dfopscoreatm['CallPremiumIncreasing'] = 'True'
        else:
            dfopscoreatm['PutPremiumIncreasing'] = 'True'

    if (dfreference['LTP_Put'] >= dfopscoreatm['lastPrice_Put']).all():
        if (dfopscoreatm['lastPrice_Call'] <= dfopscoreatm['lastPrice_Put']).all():
            dfopscoreatm['CallPremiumDecreasing'] = 'True'
        else:
            dfopscoreatm['PutPremiumDecreasing'] = 'True'
    else:
        if (dfopscoreatm['lastPrice_Call'] >= dfopscoreatm['lastPrice_Put']).all():
            dfopscoreatm['CallPremiumIncreasing'] = 'True'
        else:
            dfopscoreatm['PutPremiumIncreasing'] = 'True'

    ##################Decay Calculations End###########################

    ###########Fresh positions writers active #################

    if ((dfopscoreatm['openInterest_Call'] >= dfreference['OI_Call']).all() & (
            dfopscoreatm['CallPremiumIncreasing'] == 'True').all()):
        dfopscoreatm['CallBuyersActive'] = 'FP'
        #   dfopscoreatm['Fresh'] = 'TrueFresh'

    if ((dfopscoreatm['openInterest_Call'] >= dfreference['OI_Call']).all() & (
            dfopscoreatm['CallPremiumDecreasing'] == 'True').all()):
        dfopscoreatm['CallWritersActive'] = 'FP'

    if (dfopscoreatm['openInterest_Put'] >= dfreference['OI_Put']).all() & (
            dfopscoreatm['PutPremiumIncreasing'] == 'True').all():
        dfopscoreatm['PutBuyersActive'] = 'FP'
        #    dfopscoreatm['Fresh'] = 'TrueFresh'

    if (dfopscoreatm['openInterest_Put'] >= dfreference['OI_Put']).all() & (
            dfopscoreatm['PutPremiumDecreasing'] == 'True').all():
        dfopscoreatm['PutWritersActive'] = 'FP'
        #    dfopscoreatm['Fresh'] = 'TrueFresh'

    ###########Fresh positions writers active End #################

    ###########Close positions writers active #################

    if (dfopscoreatm['openInterest_Call'] < dfreference['OI_Call']).all() & (
            dfopscoreatm['CallPremiumIncreasing'] == 'True').all():
        dfopscoreatm['CallWritersActive'] = 'CP'
        #  dfopscoreatm['Close']='TrueClose'

    if (dfopscoreatm['openInterest_Put'] < dfreference['OI_Put']).all() & (
            dfopscoreatm['PutPremiumIncreasing'] == 'True').all():
        dfopscoreatm['PutWritersActive'] = 'CP'
        # dfopscoreatm['Close'] = 'TrueClose'

        ###########Close positions buyers active #################
    if (dfopscoreatm['openInterest_Call'] < dfreference['OI_Call']).all() & (
            dfopscoreatm['CallPremiumDecreasing'] == 'True').all():
        dfopscoreatm['CallBuyersActive'] = 'CP'
        # positions['Close'] = 'TrueClose'

    if (dfopscoreatm['openInterest_Put'] < dfreference['OI_Put']).all() & (
            dfopscoreatm['PutPremiumDecreasing'] == 'True').all():
        dfopscoreatm['PutBuyersActive'] = 'CP'
        # dfopscoreatm['Close'] = 'TrueClose'

    try:

        ##################IV Conclusion ###########################
        if ((dfopscoreatm['impliedVolatility_Call'] > dfreference['IV_Call']).all() & (
                dfopscoreatm['CallBuyersActive'] == 'True').all()):
            dfopscoreatm['CallBuyersActive'] = 'SmallPlayersIV'

        if ((dfopscoreatm['impliedVolatility_Put'] > dfreference['IV_Put']).all() & (
                dfopscoreatm['PutBuyersActive'] == 'True').all()):
            dfopscoreatm['PutBuyersActive'] = 'SmallPlayersIV'

        if ((dfopscoreatm['impliedVolatility_Call'] <= dfreference['IV_Call']).all() & (
                dfopscoreatm['CallBuyersActive'] == 'True').all()):
            dfopscoreatm['CallWritersActive'] = 'BigPlayersIV'
            dfopscoreatm['CallBuyersActive'] = 'False'

        if ((dfopscoreatm['impliedVolatility_Put'] <= dfreference['IV_Put']).all() & (
                dfopscoreatm['PutBuyersActive'] == 'True').all()):
            dfopscoreatm['PutWritersActive'] = 'BigPlayersIV'
            dfopscoreatm['PutBuyersActive'] = 'False'

        ##################Volume Conclusion ###########################
        if ((dfopscoreatm['totalTradedVolume_Call'] > dfreference['Volume_Call']).all() & (
                dfopscoreatm['CallPremiumIncreasing'] == 'True').all()):
            dfopscoreatm['CallBuyersActive'] = 'TrueVOl'

        if ((dfopscoreatm['totalTradedVolume_Put'] > dfreference['Volume_Put']).all() & (
                dfopscoreatm['PutPremiumIncreasing'] == 'True').all()):
            dfopscoreatm['PutBuyersActive'] = 'TrueVOL'

        if ((dfopscoreatm['totalTradedVolume_Call'] <= dfreference['Volume_Call']).all() & (
                dfopscoreatm['CallPremiumDecreasing'] == 'True').all()):
            dfopscoreatm['CallWritersActive'] = 'TrueVOL'

        if ((dfopscoreatm['totalTradedVolume_Put'] <= dfreference['Volume_Put']).all() & (
                dfopscoreatm['PutPremiumDecreasing'] == 'True').all()):
            dfopscoreatm['PutWritersActive'] = 'TrueVOL'
            dfopscoreatm['PutBuyersActive'] = 'FalseVOl'
    except Exception as error:
        print("Error while checking conditions. Error : {0}".format(error))
        quit()

    return dfopscoreatm


def zscoremain():
    # def main():
    global df_list
    df_nearatmcdata = pd.DataFrame()
    df_nearatmpdata = pd.DataFrame()
    dfzscorerec = pd.DataFrame()

    try:

        nearatmcall_list = json.loads(open(nearatmcall_filename).read())
        nearatmcall_df = pd.DataFrame().from_dict(nearatmcall_list)

        zscore_list = json.loads(open(zscore_filename).read())
        dfzscorerec = pd.DataFrame().from_dict(zscore_list)

    except Exception as error:
        print("Error reading data. Error : {0}".format(error))
        nearatmcall_df = pd.DataFrame()

    nearatmcall_df, df_nearatmcdata, dfzscorerec = \
        nearatmstrikes(nearatmcall_df, df_nearatmcdata, dfzscorerec)

    wb.sheets['NearATMDatacall'].range("A1").options(header=True).value = nearatmcall_df
    wb.sheets['zscore'].range("A1").options(header=True).value = dfzscorerec
    wb.sheets['zscoredash'].range("A1").options(header=True).value = dfzscorerec[
        ['CallBuyersActive', 'CallPremiumIncreasing', 'CallPremiumDecreasing', 'CallWritersActive', 'PutBuyersActive',
         'PutPremiumIncreasing', 'PutPremiumDecreasing', 'PutWritersActive', 'call_score', 'put_score', 'score',
         'underlying']]

    ###recording strikepricewise changeing data###
    print('###############writing to file Starts here################')
    with open(nearatmcall_filename, "w") as files:
        files.write(json.dumps(nearatmcall_df.to_dict(), cls=NpEncoder, indent=4, sort_keys=True))

    with open(zscore_filename, "w") as files:
        files.write(json.dumps(dfzscorerec.to_dict(), cls=NpEncoder, indent=4, sort_keys=True))

    print('###############writing to file Ends here################')
    # wb.api.RefreshAll()


if __name__ == '__main__':
    zscoremain()

