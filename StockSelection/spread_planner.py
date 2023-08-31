from tkinter import *
import requests
import pandas as pd
import numpy as np
from jproperties import Properties


# read these constants from properties TODO
spotsymbol='NIFTY 50'
symbol ='NIFTY'
expiry = '30JUL2020'

class Window(Frame):

    def __init__(self, master=None):
        Frame.__init__(self, master)
        self.master = master
        self.master.title("Spread Planner")
        self.pack(fill=BOTH, expand=1)

        self.frame1 = Frame(self)
        self.frame1.pack()

        self.frame2 = Frame(self)
        self.frame2.pack()

        self.optionTypeLbl = Label(self.frame1, text="Option Type").grid(row=1, column=0)
        #optionTypeLbl = Label(self.frame1, text="Option Type")

        self.optionTypeVar = StringVar(self)
        self.optionTypeVar.set("Select")
        self.optionTypeMenu = OptionMenu(self.frame1, self.optionTypeVar, "Call", "Put", command=self.getPremium)
        self.optionTypeMenu.grid(row=2, column=0)

        self.tradeTypeLbl = Label(self.frame1, text="Trade Type").grid(row=1, column=1)
        self.tradeTypeVar = StringVar(self)
        self.tradeTypeVar.set("Select")
        self.tradeTypeMenu = OptionMenu(self.frame1, self.tradeTypeVar, "Buy", "Sell", command=self.getPremium)
        self.tradeTypeMenu.grid(row=2, column=1)


        self.strikePriceLbl = Label(self.frame1, text="Strike Price").grid(row=1, column=3)
        #self.strikePriceLbl = Label(self, text="Strike Price")
        #self.strikePriceLbl.pack()
        self.strikePriceVar = StringVar(self)
        self.strikePriceTxtField = Entry(self.frame1, textvariable=self.strikePriceVar)
        self.strikePriceTxtField.bind("<FocusOut>", self.getPremium)
        self.strikePriceTxtField.grid(row=2, column=3)
        #self.strikePriceTxtField.pack()

        self.premiumLbl = Label(self.frame1, text="Premium").grid(row=1, column=4)
        #self.premiumLbl = Label(self, text="Premium")
        #self.premiumLbl.pack()
        self.premiumVar = StringVar(self)
        self.premiumTxtField = Entry(self.frame1, textvariable=self.premiumVar)
        self.premiumTxtField.grid(row=2, column=4)
        #self.premiumTxtField.pack()

        self.qtyLbl = Label(self.frame1, text="Quantity").grid(row=1, column=5)
        #self.qtyLbl = Label(self, text="Quantity")
        #self.qtyLbl.pack()
        self.qtyVar = StringVar(self)
        self.qtyTxtField = Entry(self.frame1, textvariable=self.qtyVar)
        self.qtyTxtField.bind("<Key>", self.calculate_premium)
        self.qtyTxtField.grid(row=2, column=5)
        #self.qtyTxtField.pack()
        self.addTradeButton = Button(self.frame1, text="Add Trade", command=self.add_trade)
        self.addTradeButton.grid(row=2, column=6)
        #self.addTradeButton.pack()

        self.strikePriceList = []
        #self.tradeList = np.array()
        self.strikePriceVar.set(self.niftyspotfun())

        #self.table = Treeview()
        #self.table["columns"] = (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17)
        #self.table.pack()
        self.table_data = []

    def remove_trade(self, row):
        print(row-1)
        print("called")
        self.table_data.pop(row - 1)
        self.table_data.pop(len(self.table_data)-1)

        self.frame2.destroy()
        self.frame2 = Frame(self)
        self.frame2.pack()
        self.drawTable()



    def calculate_premium(self, event):
        if self.premiumVar.get() != '' and self.qtyVar.get() != '':
            premium_amount = float(self.premiumVar.get()) * int(self.qtyVar.get())
            if self.tradeTypeVar.get() == 'Buy':
                premium_amount = - premium_amount
            print(premium_amount)

    def pullOC(self, event):
        optionTypeVar = self.optionTypeVar.get()
        tradeTypeVar = self.tradeTypeVar.get()
        strikePriceVar = self.strikePriceTxtField.get()
        qtyVar = self.qtyTxtField.get()
        #validate all values before to invoke nse
        if optionTypeVar != "Select" and tradeTypeVar != "Select" and strikePriceVar != '' and qtyVar != '':
            print("invoking nifty...")
            self.fetchOC(symbol, expiry)

    def niftyspotfun(self):
        headers = {
            "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/80.0.3987.100 Safari/537.36',
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "Referer": "https://www1.nseindia.com/live_market/dynaContent/live_watch/get_quote/GetQuoteFO.jsp?underlying=NIFTY&instrument=FUTIDX&expiry=7MAY2020&type=-&strike=-"}

        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"

        r = requests.get(url, headers=headers).json()

        # ce_values = [data['CE'] for data in r['records']['data'] if
        #              "CE" in data and str(data['expiryDate']).lower() == str(expiry).lower()]
        record = pd.DataFrame(r['data'])

        record = record.filter(['symbol', 'lastPrice'])
        nifty = record[record.symbol == spotsymbol]
        nifty_spot_price = round(int(nifty['lastPrice'].values), 0)
        atm_price = nifty_spot_price - divmod(nifty_spot_price, 50)[1]
        return atm_price




    def add_trade(self):

        if len(self.table_data) == 0:
            self.table_data.append(self.strikePriceList)
        else:
            self.table_data.pop(len(self.table_data)-1)

        row = []
        print("adding trade in list...")
        ce_or_pe = ''
        if self.optionTypeVar.get() == 'Call':
            ce_or_pe = 'CE'
        else:
            ce_or_pe = 'PE'
        trade_notation = symbol+'-'+expiry+'-'+self.strikePriceVar.get()+ce_or_pe
        row.append(trade_notation)
        print(symbol+'-'+expiry+'-'+self.strikePriceVar.get()+ce_or_pe)

        selected_strike_price = int(self.strikePriceVar.get())
        selected_premium_val = float(self.premiumVar.get())

        if ce_or_pe == 'CE' and self.tradeTypeVar.get() == 'Buy':
            # For CE Buy
            for sp in self.strikePriceList:
                lp_at_sp = sp - selected_strike_price
                if sp <= selected_strike_price:
                    lp_at_sp = - selected_premium_val
                else:
                    lp_at_sp = sp - selected_strike_price - selected_premium_val

                qty = self.qtyVar.get()
                row.append(int(lp_at_sp * int(qty)))

        elif ce_or_pe == 'CE' and self.tradeTypeVar.get() == 'Sell':
            # For CE Sell
            for sp in self.strikePriceList:
                lp_at_sp = selected_strike_price - sp
                if sp <= selected_strike_price:
                    lp_at_sp = selected_premium_val
                else:
                    lp_at_sp = selected_premium_val - (sp - selected_strike_price)

                row.append(int(lp_at_sp * int(self.qtyVar.get())))
        elif ce_or_pe == 'PE' and self.tradeTypeVar.get() == 'Buy':
            # For PE Buy
            for sp in self.strikePriceList:
                lp_at_sp = selected_strike_price - selected_premium_val - sp
                if sp <= selected_strike_price:
                    lp_at_sp = selected_strike_price - selected_premium_val - sp
                else:
                    lp_at_sp = -selected_premium_val

                row.append(int(lp_at_sp * int(self.qtyVar.get())))
        elif ce_or_pe == 'PE' and self.tradeTypeVar.get() == 'Sell':
            # For PE Sell
            for sp in self.strikePriceList:
                if sp <= selected_strike_price:
                    lp_at_sp = sp - (selected_strike_price - selected_premium_val)
                else:
                    lp_at_sp = selected_premium_val

                row.append(int(lp_at_sp * int(self.qtyVar.get())))
        self.table_data.append(row)
        self.drawTable()


    def drawTable(self):
        last_row = []
        for column in range(len(self.strikePriceList)):
            one_column_sum = 0
            for row in range(len(self.table_data)):
                if row != 0 and column != 0:
                    print(self.table_data[row][column])
                    one_column_sum = one_column_sum + int(self.table_data[row][column])
            last_row.append(one_column_sum)
        self.table_data.append(last_row)
        for row in range(len(self.table_data)):
            for column in range(len(self.strikePriceList)):
                if row == 0:
                    if column == 0:
                        label = Label(self.frame2, text="# Trade", pady=2)
                    else:
                        label = Label(self.frame2, text=self.strikePriceList[column-1], pady=2)
                        label.config(font=('Arial', 12))

                        label.grid(row=row+3, column=column)
                    #label.pack()
                elif row == len(self.table_data) -1:
                    if column == 0:
                        label = Label(self.frame2, text="Total P/L", pady=2)
                        label.config(font=('Arial', 14), bg="green")
                        label.grid(row=row + 3, column=column)
                    label = Label(self.frame2, text=self.table_data[row][column], pady=2)
                    label.config(font=('Arial', 14), bg="green")
                    label.grid(row=row + 3, column=column)
                else:
                    label = Label(self.frame2, text=self.table_data[row][column], pady=2)
                    label.config(font=('Arial', 12))
                    label.grid(row=row+3, column=column)
                    if column == len(self.strikePriceList) - 1:
                        button = Button(self.frame2, text="Delete", command=lambda: self.remove_trade(row))
                        button.grid(row=row + 3, column=column + 1)

    def fetchOC(self, symbol, expiry):
        headers = {
            "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/80.0.3987.100 Safari/537.36',
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "Referer": "https://www1.nseindia.com/products/content/equities/equities/eq_security.htm"}

        url = (
                "https://www1.nseindia.com/live_market/dynaContent/live_watch/option_chain/optionKeys.jsp?symbolCode=-"
                "&symbol=" + symbol + "&instrument=-&date=" + expiry + "&segmentLink=17&symbolCount=2&segmentLink=17")
        r = requests.get(url, headers=headers).text
        nseoption = pd.read_html(r)
        dlist = nseoption[1]
        niftystr = nseoption[0][1]
        str = " "
        strlist = list(niftystr)
        str = str.join(strlist)
        #print(df)
        return dlist

    def getPremium(self, event):
        optionTypeVar = self.optionTypeVar.get()
        tradeTypeVar = self.tradeTypeVar.get()
        strikePriceVar = self.strikePriceTxtField.get()
        # validate all values before to invoke nse
        if optionTypeVar != "Select" and tradeTypeVar != "Select" and strikePriceVar != '':
            print("invoking nifty...")
            print("getting premium")
            dlist = self.fetchOC(symbol, expiry)
            atm_price = self.niftyspotfun()
            df = pd.DataFrame()
            df.reset_index(inplace=True)
            df1 = pd.DataFrame(dlist['Unnamed: 11_level_0'])
            df2 = pd.DataFrame(dlist['CALLS'])
            df3 = pd.DataFrame(dlist['PUTS'])
            df1.columns = df1.columns.str.replace(' ', '_')
            df3.columns = df3.columns.str.replace(' ', '_')
            df2.columns = df2.columns.str.replace(' ', '_')
            if self.optionTypeVar.get() == 'Call':
                #df = df2.join(df3, how='left', lsuffix='_Call', rsuffix='_Put')
                df = df2.join(df1, how='left')
            else:
                 df = df3.join(df1, how="left")
            strike_price_row = df['Strike_Price'].values == int(strikePriceVar)
            df_strikePrice = df[strike_price_row]

        self.premiumVar.set(df_strikePrice['LTP'].values[0])

        four_hundred_atm_price = df['Strike_Price'].values > (atm_price - 500)
        df = df[four_hundred_atm_price]
        df.reset_index(inplace=True)
        atm_price_four_hundred = df['Strike_Price'].values < (atm_price + 500)
        df =df[atm_price_four_hundred]
        self.strikePriceList = df['Strike_Price'].tolist()
        self.strikePriceList = [round(x) for x in self.strikePriceList]




root = Tk()

width, height = root.winfo_screenwidth(), root.winfo_screenheight()

root.geometry('%dx%d+0+0' % (width,height))
#root.geometry("1200x900")

app = Window(root)

root.mainloop()