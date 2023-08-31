import datetime
from datetime import time, timedelta
import time
import requests
import pandas as pd
from nsepython import nse_holidays
from selenium import webdriver
import urllib3
import json
import os
import configparser
import nsepython

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

weekdays = [5, 6]

config = configparser.RawConfigParser()
config.read('ConfigFile.properties')
path = config.get('filedetails', 'filelocation')


# path='/Users/E1360827/Files'

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


def get_session_cookies():
    print(path)

    driver = webdriver.Chrome(executable_path=os.path.join(path, "chromedriver.exe"))         # Path is projectPy/files
    driver.get("https://www.nseindia.com")
    cookies = driver.get_cookies()

    cookie_dict = {}
    try:
        print('Opening File cookies')
        with open('cookies', 'w') as line:
            print('Opened File cookies')
            for cookie in cookies:
                cookie_dict[cookie['name']] = cookie['value']
                print('Writing cookies')
            line.write(json.dumps(cookie_dict, cls=NpEncoder, indent=4, sort_keys=True))
        print('cookies writing/refresh completed')
        driver.quit()
        return cookie_dict
    except Exception as error:
        print('error occurred in writing cookies')
        driver.quit()


import math


def roundx(x, base=50):
    print('in function round up')
    return int(base * round(float(x) / base))

"""
def lastworkday():
    last_Bus_Day = datetime.datetime.today()
    for last_Bus_Day in nse_holidays(type="trading"):
        last_Bus_Day = last_Bus_Day - datetime.timedelta(days=1)
    print(last_Bus_Day)


def lastworkingday():
    lastBusDay = datetime.datetime.today()
    if datetime.date.weekday(lastBusDay) == 5 or datetime.date.weekday(lastBusDay) in holidays():  # if it's Saturday
        lastBusDay = lastBusDay - datetime.timedelta(days=1)  # then make it Friday
    elif datetime.date.weekday(lastBusDay) == 6:  # if it's Sunday
        lastBusDay = lastBusDay - datetime.timedelta(days=2);  # then make it Friday
    print(lastBusDay)
"""

def holidays():
    url = "https://www.nseindia.com/api/holiday-master?type=trading"

    headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/80.0.3987.100 Safari/537.36',
        "Accept-Language": "en-US,en;q=0.9",
        # "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.nseindia.com/resources/exchange-communication-holidays"}
    r = requests.get(url, headers=headers).json()
    dfHolidays = pd.DataFrame(r['CBM'])
    holidaysList = pd.to_datetime(dfHolidays['tradingDate']).dt.strftime('%d%m%Y')
    # print(holidaysList)
    return holidaysList


def retry(fun, max_tries=10):
    for i in range(max_tries):
        print('retrying1...')
        try:
            time.sleep(0.3)
            print('retrying...')
            fun()
            break
        except Exception:
            print('Exception retrying')
            print("error {0}".format(error))
            continue


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def LastThInMonth(year, month):
    import calendar
    import datetime
    # Create a datetime.date for the last day of the given month
    daysInMonth = calendar.monthrange(year, month)[1]  # Returns (month, numberOfDaysInMonth)
    dt = datetime.date(year, month, daysInMonth)

    # Back up to the most recent Thursday
    offset = 4 - dt.isoweekday()
    if offset > 0:
        offset -= 7  # Back up one week if necessary
    dt += datetime.timedelta(offset)  # dt is now date of last Th in month
    # Throw an exception if dt is in the current month and occurred before today
    now = datetime.date.today()  # Get current date (local time, not utc)
    if dt.year == now.year and dt.month == now.month + 1 and dt < now:
        raise Exception('Oops - missed the last Thursday of this month')

    return dt


def niftyexpiry():
    url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
    # url = "https://www.nseindia.com/api/quote-derivative?symbol=NIFTY"
    headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
        # "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8", "Accept-Encoding": "gzip, deflate",
        "Referer": "https://www.nseindia.com/get-quotes/derivatives?symbol=NIFTY&identifier=OPTIDXNIFTY26-11-2020PE12800.00"}
    cookie_dict = {
        'bm_sv': '399C35EBE5B33A2AE6157217AB14E791~+wbZmQSw55+ee6qWONJHs4smJK6UXtR1mldzOjtCKXZht8dUnmKBFXYgyRm'
                 'E7dRIZgoUOxW9fUQ27xfGTnMa9mMwFPsADL9pi8caRNwRHQruYqhY5hQKB96AgSKghg0zyzAQT4IgynhMhCg3yMo1+LIFQv86PwwRgRc/AeMPgZM='}
    try:
        cookie_dict = json.loads(open('cookies').read())
    except Exception as error:
        print("Error reading cookies most nifty expiry")
        cookie_dict = get_session_cookies()
    session = requests.session()
    for cookie in cookie_dict:
        if "bm_sv" in cookie or 'nseappid' in cookie or "nsit" in cookie:
            session.cookies.set(cookie, cookie_dict[cookie])
    try:
        r = session.get(url, headers=headers, verify=False).json()
    except Exception as error:
        print("error in reading cookies. Error : {0}".format(error))
        for cookie in cookie_dict:
            if "bm_sv" in cookie or 'nseappid' in cookie or "nsit" in cookie:
                session.cookies.set(cookie, cookie_dict[cookie])
        r = session.get(url, headers=headers, verify=False).json()
    expiryDates = r['records']['expiryDates']

    return expiryDates


def append_xl(df, excel_file, cols, sheetnanme, indexcol):
    # df1 = pd.read_excel(excel_file,usecols=cols, sheet_name=sheetnanme, headers=True, index_col=indexcol)
    df1 = pd.read_excel(excel_file, usecols=cols, sheet_name=sheetnanme, headers=True)
    df1 = df1.append(df, sort=False)
    return df1


def main():
    # print('nifty spot')
    # roundx(15108)
    #  print( int(100 * round(float(15108)/100)))
    get_session_cookies()


if __name__ == '__main__':
    main()
