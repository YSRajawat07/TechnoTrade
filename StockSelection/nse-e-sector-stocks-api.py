from flask import Flask, Response, request, jsonify
from flask_cors import CORS
from IPython.display import display
from numpy import NaN, nan
import pandas as pd
# Need to handle missing values.
# from flask import Blueprint
# We will create a new file called routes.py where all our app.routes resides.
# Since we are moving all the app.routes out from the app.py file,
# our app.route will not work anymore.
# To keep everything intact and our application up and running, Blueprint comes into the picture
# Blueprint


import pymongo
import json
from bson.objectid import ObjectId


app = Flask(__name__)
CORS(app)
# app.config.from_pyfile('nseapp.cfg')
# DB connections

try:
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
    niftydb = client["NiftyDB"]
except Exception as ex:
    print(ex)
print('DB connection established')
##################################################################
# For STOCKS


@app.route('/stocks/<string:stockname>')
def getStockData(stockname):
    try:
        dbresponse = list(niftydb.nsestocks.find({"symbol": {"$eq": stockname}}))
        dbstockdata=[]
        for data in dbresponse:
            dbstockdata.append(({"symbol": data['symbol'],
                                 "open": data['open'],
                                 "dayHigh": data['dayHigh'],
                                 "dayLow": data['dayLow'],
                                 "lastPrice": data['lastPrice'],
                                 "previousClose": data['previousClose'],
                                 "change": data['change'],
                                 "ffmc": data['ffmc'],
                                 "yearHigh": data['yearHigh'],
                                 "yearLow": data['yearLow'],
                                 "pChange": data['pChange'],
                                 "perChange365d": data['perChange365d'],
                                 "totalTradedVolume": data['totalTradedVolume'],
                                 "totalTradedValue": data['totalTradedValue'],
                                 "lastUpdateTime": data['lastUpdateTime'],
                                 "nearWKH": data['nearWKH']
                                 }))
        return Response(
            response=json.dumps(dbstockdata),
            status=200,
            mimetype='application/json'
        )
    except Exception as ex:
        print(ex)
        return Response(response=json.dumps({"message": "cannot read response"}),status=500,mimetype='application/json')

##################################################################
# For SECTORS
def datefunction(e):
    monthMap = {'Jan':1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}
    dateAndTime = e['currentDate'].split(' ')
    timeInfo = dateAndTime[0].split('-')
    year = int(timeInfo[2])
    month = monthMap[timeInfo[1]]
    date = int(timeInfo[0])
    return year,month,date


@app.route('/sector/<string:sectorname>')
def getSectorData(sectorname):
    try:
        # niftydb.nsesectors.sort({"currentDate": -1})
        dbresponse = list(niftydb.nsesectors.find({"index": {"$eq": sectorname}}))
        dbresponse.sort(reverse=True, key=datefunction)
        df = pd.DataFrame(dbresponse)
        if len(df.index) != 0:
            df['perChangeWeek'] = round(df['last'].diff(periods=-7), 4)
        df = df.fillna(0)
        dbresponse1 = df.to_dict(orient='records')
        dbsectordata = []
        for data in dbresponse1:
            if 'oneYearAgo' not in data: data['oneYearAgo'] = 0.0
            if 'perChangeWeek' not in data: data['perChangeWeek'] = 0.0
            dbsectordata.append(({"index": data['index'],
                                  "last": data['last'],
                                  "open": data['open'],
                                  "high": data['high'],
                                  "low": data['low'],
                                  "previousClose": data['previousClose'],
                                  "variation": data['variation'],
                                  "percentChange": data['percentChange'],
                                  "declines": data['declines'],
                                  "advances": data["advances"],
                                  "perChange365d": data['perChange365d'],
                                  "perChange30d": data['perChange30d'],
                                  "date365dAgo": data['date365dAgo'],
                                  "date30dAgo": data['date30dAgo'],
                                  "previousDay": data['previousDay'],
                                  "oneWeekAgo": data['oneWeekAgo'],
                                  "oneMonthAgo": data['oneMonthAgo'],
                                  "oneYearAgo": data['oneYearAgo'],
                                  "currentDate": data['currentDate'].split(' ')[0],
                                  "perChangeWeek": data['perChangeWeek']
                                  }))
        dbsectordata.sort(reverse=True, key=datefunction)
        return Response(
            response=json.dumps(dbsectordata),
            status=200,
            mimetype='application/json'
        )
    except Exception as ex:
        print(ex)
        return Response(response=json.dumps({"message": "cannot read response"}), status=500,
                        mimetype='application/json')


if __name__ == '__main__':
    app.run(debug=True)