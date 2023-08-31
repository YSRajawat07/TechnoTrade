from scipy.stats import norm as stats
from NseDataFetch import *
from NseIndexLive import getoptionchain
import configparser
# Declaration of files and display setting for panda

config = configparser.RawConfigParser()
config.read('ConfigFile.properties')

path =config.get('filedetails', 'filelocation')
filename = config.get('filedetails','NseIndexDataDaily.filename')
excel_file = path+filename
wb = xw.Book(excel_file)
sheet_oi_single = wb.sheets("OIData")

symbol = 'NIFTY'
spotsymbol = 'NIFTY 50'
vixymbol = 'INDIA VIX'

sht_live = wb.sheets("Data")

niftyindexOI = os.path.join("/Users/ajaysinghrajawat/Documents/pythonprojects/Files/EOD/backup",
                            "nseindexeod_{0}".format(datetime.now().strftime("%m%y")))
niftyfuturerecords = os.path.join("/Users/ajaysinghrajawat/Documents/pythonprojects/Files/EOD/backup",
                                  "nsefuteod_{0}".format(datetime.now().strftime("%m%y")))
callatmrecords = os.path.join("/Users/ajaysinghrajawat/Documents/pythonprojects/Files/EOD/backup",
                              "nseindexcalleod_{0}".format(datetime.now().strftime("%m%y")))
putatmrecords = os.path.join("/Users/ajaysinghrajawat/Documents/pythonprojects/Files/EOD/backup",
                             "nseindexputeod_{0}".format(datetime.now().strftime("%m%y")))
callhighoirecords = os.path.join("/Users/ajaysinghrajawat/Documents/pythonprojects/Files/EOD/backup",
                                 "nseindexcallhighoieod_{0}".format(datetime.now().strftime("%m%y")))
puthighoirecords = os.path.join("/Users/ajaysinghrajawat/Documents/pythonprojects/Files/EOD/backup",
                                "nseindexputhighoieod_{0}".format(datetime.now().strftime("%m%y")))
##Date format without 0 inn front for single digit day e.g "4JUN2020"

dt = datetime.today()

expiry = niftyexpiry()

expiry = expiry[0]

lth= LastThInMonth(dt.year,dt.month)

expiry_fut =lth.strftime('%d-%b-%Y')
indexval = 0

df_list = []
mp_list = []

mpdict_activecall = []
mpdict_activeput = []

#  span = n largest open interest values
span = 3
largestcall = 'openInterest_Call'
largestput = 'openInterest_Put'

timeframe = 5
interval = 12
start = time(9, 15)
end = time(15, 33)

niftyspot = 0000.00
niftyvolume = 00000000

fromtime = datetime(2020, 7, 17, 15, 30)
totime = datetime(2020, 7, 23, 15, 30)
t1 = totime - fromtime
daystoexpiry = t1 / timedelta(days=1)

pd.set_option('display.width', 1500)
pd.set_option('display.max_columns', 75)
pd.set_option('display.max_rows', 1500)

nifty = pd.DataFrame()

wb.sheets['FutureData'].range('2:50000').clear_contents()
wb.sheets['MPDataput'].range('2:50000').clear_contents()
wb.sheets['MPDatacall'].range('2:50000').clear_contents()



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

def main():
    global df_list, dffut
    global mpdict_activecall
    global mpdict_activecput
    waitsecs = 0
    # niftyspotfun()
    # mostactive()

    df_cdata = pd.DataFrame()
    df_pdata = pd.DataFrame()
    df_diff = pd.DataFrame()
    dffut = pd.DataFrame()

    try:
        df_list = json.loads(open(niftyindexOI).read())
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
        fut_list = json.loads(open(niftyfuturerecords).read())
        df_fut = pd.DataFrame().from_dict(fut_list)
    except Exception as error:
        print("Error reading data. Error : {0}".format(error))
        fut_list = []
        df_fut = pd.DataFrame()

    ################

    try:
        mpcall_list = json.loads(open(callatmrecords).read())
        # mp_list = ast.literal_eval(mp_list)
        mpcall_df = pd.DataFrame().from_dict(mpcall_list)

        mpput_list = json.loads(open(putatmrecords).read())
        # mp_list = ast.literal_eval(mp_list)
        mpput_df = pd.DataFrame().from_dict(mpput_list)

    except Exception as error:
        print("Error reading data. Error : {0}".format(error))
        mpcall_df = pd.DataFrame()
        mpput_df = pd.DataFrame()
    try:

        df_cdata = append_xl(df_cdata, excel_file,"A:Q", 'CallData', 'Time')

    except Exception as error:
        print("Error reading data. Error : {0}".format(error))
        df_cdata = pd.DataFrame()

    try:
        df_pdata = append_xl(df_pdata, excel_file,"A:Q",'PutData', 'Time')
        print(df_pdata)
    except Exception as error:
        print("Error reading data. Error : {0}".format(error))
        df_pdata = pd.DataFrame()

    ########################Furture data collection############

    df_fut = getlivefuture(df_fut)

    wb.sheets['FutureData'].range("A1").options(header=True).value = df_fut

    with open(niftyfuturerecords, "w") as files:
        files.write(json.dumps(df_fut.to_dict(), cls=NpEncoder, indent=4, sort_keys=True))

    ###################### Furture data collection ends here###########

    ########################Option Chain data collection############
    df, mpcall_df, mpput_df, df_cdata, df_pdata, df_diff = \
        getoptionchain(df, mpcall_df, mpput_df, df_cdata, df_pdata, df_diff)

    wb.sheets['MPDatacall'].range("A1").options(header=True).value = mpcall_df
    wb.sheets['MPDataput'].range("A1").options(header=True).value = mpput_df

    ###recording strikepricewise changeing data###

    print("printing Df_diff test End")

    with open(callatmrecords, "w") as files:
        files.write(json.dumps(mpcall_df.to_dict(), cls=NpEncoder, indent=4, sort_keys=True))
        # wb.api.RefreshAll() with open(mp_filename, "w") as files:

    with open(putatmrecords, "w") as files:
        files.write(json.dumps(mpput_df.to_dict(), cls=NpEncoder, indent=4, sort_keys=True))
        # wb.api.RefreshAll()

    df_list.append(df.to_dict('records'))

    with open(niftyindexOI, "w") as files:
        files.write(json.dumps(df_list, indent=4, sort_keys=True))
    ###################### Option Chain data collection ends here###########

    wb.save()

    if not df.empty:
        df['impliedVolatility_Call'] = df['impliedVolatility_Call'].replace(to_replace=0, method='bfill').values
        df['impliedVolatility_Put'] = df['impliedVolatility_Put'].replace(to_replace=0, method='bfill').values
        df['datetime'] = datetime.now().strftime("%d-%m-%y%y %H:%M")

        sht_live.range("A2").options(header=True, index=False).value = df

    else:
        print("No data received")


if __name__ == '__main__':
    main()
