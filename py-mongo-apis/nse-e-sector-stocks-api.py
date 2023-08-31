from flask import Flask, Response, request, jsonify

# from flask import Blueprint
# We will create a new file called routes.py where all our app.routes resides.
# Since we are moving all the app.routes out from the app.py file,
# our app.route will not work anymore.
# To keep everything intact and our application up and running, Blueprint comes into the picture
# Bluepprint
import pymongo
import json
from bson.objectid import ObjectId
app = Flask(__name__)
# app.config.from_pyfile('nseapp.cfg')
# DB connections
try:
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
    niftydb = client["NiftyDB"]
except Exception as ex:
    print(ex)
print('DB connection established')
##################################################################


@app.route('/stocks/<string:stockname>')
def getStockData(stockname):
    try:
        dbresponse = list(niftydb.nsestocks.find({"symbol": {"$eq": stockname}}))
        dbstockdata=[]
        for data in dbresponse:
          dbstockdata.append(({'symbol':data['symbol'],
                               "dayHigh": data['dayHigh'],
                               "dayLow": data['dayLow'],
                               "lastPrice": data['lastPrice'],
                               "previousClose": data['previousClose'],
                               "change": data['change'],
                               "ffmc": data['ffmc'],
                               "yearHigh": data['yearHigh'],
                               "yearLow": data['yearLow'],
                               "pChange": data['pChange']
                               }))
        return Response (
            response = json.dumps (dbstockdata),
            status=200,
            mimetype ='application/json'
        )
    except Exception as ex:
        print(ex)
        return Response(response=json.dumps({"message": "cannot read response"}), status=500,
                        mimetype='application/json')
##################################################################


if __name__ == '__main__':
    app.run(debug=True)
