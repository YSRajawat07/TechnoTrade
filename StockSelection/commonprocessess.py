def stocksdataprocess(datareturn, list):
    datareturn.columns = datareturn.columns.str.replace(' ', '')

    for data in list:
        try:

            datareturn['EMA20'] = datareturn.Close.ewm(span=20, adjust=False).mean()
            datareturn['EMA50'] = datareturn.Close.ewm(span=50, adjust=False).mean()
            datareturn['EMA100'] = datareturn.Close.ewm(span=100, adjust=False).mean()
            datareturn['EMA200'] = datareturn.Close.ewm(span=200, adjust=False).mean()

            datareturn['EMA20-50'] = datareturn['EMA20'] - datareturn['EMA50']
            # price percentage above 20 ema
            datareturn['per20EMA'] = (datareturn['Close'] / datareturn['EMA20']) * 100

            datareturn['MA20'] = datareturn.Close.rolling(window=20).mean()
            datareturn['20dSTD'] = datareturn.Close.rolling(window=20).std()
            datareturn['Upper'] = datareturn['MA20'] + (datareturn['20dSTD'] * 2)
            datareturn['Lower'] = datareturn['MA20'] - (datareturn['20dSTD'] * 2)

            # Delivery % Data#####

            datareturn['Value'] = datareturn.Volume * datareturn.Close

            datareturn['3Value'] = datareturn.Value.rolling(3).mean() / million
            datareturn['5Value'] = datareturn.Value.rolling(5).mean() / million
            datareturn['8Value'] = datareturn.Value.rolling(8).mean() / million
            datareturn['13Value'] = datareturn.Value.rolling(13).mean() / million

            datareturn['Value'] = datareturn['Value'] / million

            datareturn['Val%'] = (datareturn['Value'] / datareturn['3Value']) * 100

            datareturn['3DeliveryQty'] = datareturn.DeliverableVolume.rolling(3).sum()
            datareturn['3Volume'] = datareturn.Volume.rolling(3).sum()

            datareturn['5DeliveryQty'] = datareturn.DeliverableVolume.rolling(5).sum()
            datareturn['5Volume'] = datareturn.Volume.rolling(5).sum()

            datareturn['8DeliveryQty'] = datareturn.DeliverableVolume.rolling(8).sum()
            datareturn['8Volume'] = datareturn.Volume.rolling(8).sum()

            datareturn['13DeliveryQty'] = datareturn.DeliverableVolume.rolling(13).sum()
            datareturn['13Volume'] = datareturn.Volume.rolling(13).sum()

            datareturn['3AvgDel%'] = (datareturn['3DeliveryQty'] / datareturn['3Volume']) * 100
            datareturn['5AvgDel%'] = (datareturn['5DeliveryQty'] / datareturn['5Volume']) * 100
            datareturn['8AvgDel%'] = (datareturn['8DeliveryQty'] / datareturn['8Volume']) * 100
            datareturn['13AvgDel%'] = (datareturn['13DeliveryQty'] / datareturn['13Volume']) * 100

            datareturn['3AvgDel%'] = datareturn['3AvgDel%'].astype(float)
            datareturn['5AvgDel%'] = datareturn['5AvgDel%'].astype(float)
            datareturn['8AvgDel%'] = datareturn['8AvgDel%'].astype(float)
            datareturn['13AvgDel%'] = datareturn['13AvgDel%'].astype(float)

            datareturn['deldirection'] = np.where((((datareturn['3AvgDel%'] >= datareturn['5AvgDel%']) &
                                                    (datareturn['5AvgDel%'] >= datareturn['8AvgDel%'])) &
                                                   (datareturn['8AvgDel%'] >= datareturn['13AvgDel%'])),
                                                  'Increasing',
                                                  (np.where((((datareturn['3AvgDel%'] <= datareturn['5AvgDel%']) &
                                                              (datareturn['5AvgDel%'] <= datareturn['8AvgDel%'])) &
                                                             (datareturn['8AvgDel%'] <= datareturn['13AvgDel%'])),
                                                            'Decreasing', 'Nothing')))

            datareturn['deldirection'] = np.where((datareturn['3AvgDel%'] >= datareturn['5AvgDel%']), 'Increasing',
                                                  (np.where((datareturn['3AvgDel%'] <= datareturn['5AvgDel%']),
                                                            'Decreasing', 'Nothing')))

            # QT/NT#####

            datareturn['TQ/NT'] = datareturn.DeliverableVolume / datareturn.Trades
            datareturn['3TQ/NT'] = datareturn.DeliverableVolume.rolling(3).sum() / datareturn.Trades.rolling(
                3).sum()
            datareturn['5TQ/NT'] = datareturn.DeliverableVolume.rolling(5).sum() / datareturn.Trades.rolling(
                5).sum()
            datareturn['8TQ/NT'] = datareturn.DeliverableVolume.rolling(8).sum() / datareturn.Trades.rolling(
                8).sum()
            datareturn['13TQ/NT'] = datareturn.DeliverableVolume.rolling(13).sum() / datareturn.Trades.rolling(
                13).sum()

            datareturn['3DeliveryQty'] = datareturn.DeliverableVolume.rolling(3).mean()
            datareturn['3Volume'] = datareturn.Volume.rolling(3).mean()

            datareturn['5DeliveryQty'] = datareturn.DeliverableVolume.rolling(5).mean()
            datareturn['5Volume'] = datareturn.Volume.rolling(5).mean()

            datareturn['8DeliveryQty'] = datareturn.DeliverableVolume.rolling(8).mean()
            datareturn['8Volume'] = datareturn.Volume.rolling(8).mean()

            datareturn['13DeliveryQty'] = datareturn.DeliverableVolume.rolling(13).mean()
            datareturn['13Volume'] = datareturn.Volume.rolling(13).mean()

            # QT/NT#####

            datareturn['3Trades'] = datareturn.Trades.rolling(3).mean()
            datareturn['5Trades'] = datareturn.Trades.rolling(5).mean()
            datareturn['8Trades'] = datareturn.Trades.rolling(8).mean()
            datareturn['13Trades'] = datareturn.Trades.rolling(13).mean()

            # Price#

            # print((datareturn.DeliverableVolume * datareturn.VWAP)/1000000)

            # print(datareturn.Close.astype(float) )

            datareturn['CashMoneyFlow'] = np.where(
                (datareturn.Close.astype(float) > datareturn.PrevClose.astype(float)),
                round(((datareturn.DeliverableVolume * datareturn.VWAP) / 1000000).astype(float), 2),
                -round(((datareturn.DeliverableVolume * datareturn.VWAP) / 1000000).astype(float), 2))
            # cash flow for last 7 days
            datareturn['cf7sum'] = datareturn.CashMoneyFlow.rolling(7).sum()
            datareturn['3Close'] = round((datareturn['Close'].rolling(3).mean()), 2)
            datareturn['5Close'] = round((datareturn['Close'].rolling(5).mean()), 2)
            datareturn['8Close'] = round((datareturn['Close'].rolling(8).mean()), 2)
            datareturn['13Close'] = round((datareturn['Close'].rolling(13).mean()), 2)
            datareturn['pricedirection'] = np.where((((datareturn['3Close'] >= datareturn['5Close']) &
                                                      (datareturn['5Close'] >= datareturn['8Close'])) &
                                                     (datareturn['8Close'] >= datareturn['13Close'])), 'Increasing',
                                                    (np.where((((datareturn['3Close'] <= datareturn['5Close']) &
                                                                (datareturn['5Close'] <= datareturn['8Close'])) &
                                                               (datareturn['8Close'] <= datareturn['13Close'])),
                                                              'Decreasing', 'Nothing')))

            # datareturn['pricedirection'] = np.where((datareturn['3Close'] >= datareturn['5Close']),'Increasing',
            #                                       (np.where((datareturn['3Close'] <= datareturn['5Close']) ,'Decreasing','Nothing')))

            # Turnover############
            datareturn['3Turnover'] = round((datareturn.Turnover.rolling(3).mean() / million), 2)
            datareturn['5Turnover'] = round((datareturn.Turnover.rolling(5).mean() / million), 2)
            datareturn['8Turnover'] = round((datareturn.Turnover.rolling(8).mean() / million), 2)
            datareturn['13Turnover'] = round((datareturn.Turnover.rolling(13).mean() / million), 2)
            # datareturn['Turnover'] =datareturn['Turnover'] / million

            datareturn['turnoverdirection'] = np.where((((datareturn['3Turnover'] >= datareturn['5Turnover']) &
                                                         (datareturn['5Turnover'] >= datareturn['8Turnover'])) &
                                                        (datareturn['8Turnover'] >= datareturn['13Turnover'])),
                                                       'Increasing',
                                                       (np.where(
                                                           (((datareturn['3Turnover'] <= datareturn['5Turnover']) &
                                                             (datareturn['5Turnover'] <= datareturn['8Turnover'])) &
                                                            (datareturn['8Turnover'] <= datareturn['13Turnover'])),
                                                           'Decreasing', 'Nothing')))

            # datareturn['turnoverdirection'] = np.where((datareturn['3Turnover'] >= datareturn['5Turnover']),'Increasing',
            #                                       (np.where((datareturn['3Turnover'] <= datareturn['5Turnover']),'Decreasing','Nothing')))

            datareturn['3Volume'] = round((datareturn['3Volume'] / million), 2)
            datareturn['5Volume'] = round((datareturn['5Volume'] / million), 2)
            datareturn['8Volume'] = round((datareturn['8Volume'] / million), 2)
            datareturn['13Volume'] = round((datareturn['13Volume'] / million), 2)
            # datareturn['Volume'] = round((datareturn['Volume']/million),2)

            datareturn['valdirection'] = np.where((((datareturn['3Value'] >= datareturn['5Value']) &
                                                    (datareturn['5Value'] >= datareturn['8Value'])) &
                                                   (datareturn['8Value'] >= datareturn['13Value'])), 'Increasing',
                                                  (np.where((((datareturn['3Value'] <= datareturn['5Value']) &
                                                              (datareturn['5Value'] <= datareturn['8Value'])) &
                                                             (datareturn['8Value'] <= datareturn['13Value'])),
                                                            'Decreasing', 'Nothing')))

            # datareturn['valdirection'] = np.where((datareturn['3Value'] >= datareturn['5Value']),'Increasing',
            #                                       (np.where((datareturn['3Value'] <= datareturn['5Value']),'Decreasing','Nothing')))

            datareturn['ATP'] = round(((datareturn['Close'] + datareturn['High'] + datareturn['Low']) / 3), 2)

            # datareturn['Vol%'] = (datareturn['Volume'] / datareturn['AvgVol'])

            datareturn['price%'] = round(
                (((datareturn['Close'] - datareturn['PrevClose']) / datareturn['PrevClose']) * 100), 2)
            datareturn['P5%'] = (datareturn['Close'] - datareturn['5Close'])
            datareturn['P3%'] = (datareturn['Close'] - datareturn['3Close'])

            datareturn.loc[datareturn['Close'] < datareturn['PrevClose'], 'Candle'] = 'RED'
            datareturn.loc[datareturn['Close'] > datareturn['PrevClose'], 'Candle'] = 'GREEN'

        except Exception as error:
            print("error {0}".format(error))
            continue

    return datareturn

