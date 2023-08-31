import pymongo
import requests

url = "mongodb://https://www1.nseindia.com/products/dynaContent/equities/indices/historicalindices.jsp"
# url = "mongodb://https://www1.nseindia.com/live_market/dynaContent/live_watch/stock_watch/foSecStockWatch.json"
# url = "mongodb://https://www.nseindia.com/market-data/live-equity-market?symbol=NIFTY%2050"

client = pymongo.MongoClient(url)
info = client.to_dict(orient='records')
db = client["NIFTY50"]
db.STOCKS.insert_many(info)
db.stocks.insert_many(info)
