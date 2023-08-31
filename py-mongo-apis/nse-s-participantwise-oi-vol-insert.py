import io
import pymongo
import xlwings as xw
from datetime import datetime, timedelta, date
from StockSelection.utility import *
import configparser
# Declaration of files and display setting for panda

config = configparser.RawConfigParser()
config.read('ConfigFile.properties')


dt = datetime.today()
sdt =datetime.today()-timedelta(days=20)
pd.options.mode.chained_assignment = None

html_file= "/Files/fii.html"

# oi_filename = os.path.join(path+"/IndexData", "OINSE_data_{0}".format(datetime.now().strftime("%m%y")))
# vol_filename = os.path.join(path+"/IndexData", "VOLNSE_data_{0}".format(datetime.now().strftime("%m%y")))
#holidays=['02042020','06042020','10042020','14042020','01052020','25052020']

df_listoi=[]
df_listvol=[]

weekdays = [5, 6]

clientType = ['FII','DII','Pro','Client']


df_oi = pd.DataFrame()
df_vol = pd.DataFrame()
df1 = pd.DataFrame()


startdate = date(sdt.year, sdt.month, sdt.day)
enddate = date(dt.year, dt.month, dt.day)

pd.set_option('display.width', 1500)
pd.set_option('display.max_columns', 75)
pd.set_option('display.max_rows', 10000)

#MongoDB connections
client = pymongo.MongoClient("mongodb://localhost:27017/")
niftydb = client["NiftyDB"]
print('DB connection established')

def main():

    global df_listoi,df_listvol,df_oi,df_vol
    df_oitemp =pd.DataFrame()
    df_voltemp =pd.DataFrame()
    for single_date in daterange(startdate, enddate):

        if single_date.weekday() not in weekdays:
            datestring = single_date.strftime("%d%m%Y")
            if datestring in list(holidays()):
                print(f"Holiday:{datestring}")
                continue
            try:
                urlvol = "https://archives.nseindia.com/content/nsccl/fao_participant_vol_"+datestring+".csv"
                urloi = "https://archives.nseindia.com/content/nsccl/fao_participant_oi_"+datestring+".csv"

                roi = requests.get(url=urloi).content
                rvol = requests.get(url=urlvol).content

                df_oitemp = pd.read_csv(io.StringIO(roi.decode('utf8')), skiprows=1)
                df_voltemp = pd.read_csv(io.StringIO(rvol.decode('utf8')), skiprows=1)
                df_oitemp['Date']=single_date.strftime("%d-%m-%Y")
                df_voltemp['Date']=single_date.strftime("%d-%m-%Y")

                niftydb.nse_participantwise_oi_data.create_index(
                    [('Client Type', pymongo.DESCENDING), ('Date', pymongo.DESCENDING)], unique=True)
                niftydb.nse_participantwise_oi_data.insert_many(df_oitemp.to_dict('records'))

                niftydb.nse_participantwise_vol_data.create_index(
                    [('Client Type', pymongo.DESCENDING), ('Date', pymongo.DESCENDING)], unique=True)
                niftydb.nse_participantwise_vol_data.insert_many(df_oitemp.to_dict('records'))

            except Exception as error:
                print("error {0}".format(error))
                continue

    print(df_oitemp.to_dict('records'))
    print(df_voltemp)
if __name__ == '__main__':
    main()
