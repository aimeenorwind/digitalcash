"""
没有限量limit，可以取很多，没有startTime限制，但是时间间隔可选值为300, 900, 1800, 7200, 14400, 86400，所以爬不了1m（没有60）
"""
import requests
import time
import pandas as pd
from digital_cash.common_functions import interval_to_milliseconds, date_to_milliseconds, convert_to_float
import os
import datetime
from Common.odpsClient import OdpsClient
from odps.models import Schema, Column, Partition
import csv
import shutil

class PoloniexRest(object):
    # 这是Binance的restful API的基础地址，不用动
    BASE_ENDPOINT = 'https://poloniex.com/public'

    symbol_list = ['BTC_BCH', 'USDT_BCH', 'USDT_BTC', 'BTC_DASH', 'USDT_DASH', 'BTC_EOS', 'USDT_EOS', 'BTC_ETC',
                   'USDT_ETC', 'BTC_ETH', 'USDT_ETH', 'USDT_LTC', 'BTC_XRP']
    # symbol_list = ['BTC_BCH', 'USDT_BCH', 'USDT_BTC', 'BTC_DASH', 'USDT_DASH', 'BTC_EOS', 'USDT_EOS', 'BTC_ETC',
    #                'USDT_ETC', 'BTC_ETH', 'USDT_ETH', 'USDT_LTC', 'BTC_XRP']
    # 可选值为300, 900, 1800, 7200, 14400, 86400
    KLINE_INTERVAL_5MINUTE = '300'
    KLINE_INTERVAL_15MINUTE = '900'
    KLINE_INTERVAL_30MINUTE = '1800'
    KLINE_INTERVAL_2HOUR = '7200'
    KLINE_INTERVAL_4HOUR = '14400'
    KLINE_INTERVAL_1DAY = '86400'

    def __init__(self):
        print("PoloniexRest class is started....")
        # self.table_name = "zz_test"
        self.table_name = "quant_digital_cash_kline_increment"
        self.poloniex_folder = "D:\\WorkSpace\\DownloadData\\DownloadDigitalCash\\Poloniex\\"
        self.csv_path = self.poloniex_folder + "latest\\"

    def get_chart_data(self, currencyPair, period, start):
        # 函数可以获取K线图数据，详见https://www.poloniex.com/support/api/

        url = self.BASE_ENDPOINT
        params = {'command': 'returnChartData',
                  'currencyPair': currencyPair,
                  'start': start,
                  'end': 9999999999,
                  'period': period}
        response = requests.get(url, params)
        # print("otherStyleTime: ", otherStyleTime, response.json())
        print('Getted data from', response.status_code, response.url)  # 打印获取数据的具体地址
        # print("response.json() result: ", response.json())
        return response.json()

    def save_to_csv(self, datalist, symbol, interval):
        isExists = os.path.exists(self.csv_path)
        if not isExists:
            os.makedirs(self.csv_path)
            print("folder is created...")
        for data in datalist:
            timeArray = time.localtime(data['date'])
            # print("data timeArray: ", timeArray)
            data['date'] = time.strftime('%Y-%m-%d %H:%M:%S', timeArray)
            data['interval'] = interval

        df = pd.DataFrame(datalist,
                          columns=['date', 'high', 'low', 'open', 'close', 'volume', 'quoteVolume', 'weightedAverage', 'interval'])

        file_name = "Poloniex_{}.csv".format(symbol)
        # print("new file_name: ", file_name)
        df.to_csv(self.csv_path + file_name, mode='a', sep=',', header=True, index=None)


    def backup_binance_csv(self, current_time):
        backup_path = self.poloniex_folder + current_time+"\\"
        source_path = self.csv_path
        shutil.move(source_path, backup_path)

    def date_to_timestamp(self, date, format_string="%Y-%m-%d %H:%M:%S"):
        # print("原始日期：", type(date))
        time_array = time.strptime(date, format_string)
        # print("datetime: ", time_array)
        time_stamp = int(time.mktime(time_array))
        # print("time stamp: ", time_stamp)
        return time_stamp

    def upload_to_odps(self, interval):
        odps_basic = OdpsClient()
        table_name = odps_basic.get_digital_table(self.table_name)
        partitions = "interval=" + interval
        table_writer = table_name.open_writer(partition=partitions, create_partition=True)
        # 获取需要上传的数据所在的文件夹
        for parent, dirnames, filenames in os.walk(self.csv_path):
            break
        for filename in filenames:
            print(filename)
            currencyPair = filename[9:].replace(".csv", "").replace("_", "")

            # 读取csv文件数据写入table
            with open(self.csv_path + filename, "r", encoding="utf-8") as csvfile:
                # 读取csv文件，返回的是迭代类型
                reader = csv.reader(csvfile)
                columns = [row for row in reader]
                # print("---增加记录数量：---", len(columns))
                csvfile.close()
                for column in columns[1:-1]:
                    column_new = ["Poloniex"]
                    column_new.append(currencyPair)
                    column_new.append(column[0])
                    column_new.append(convert_to_float(column[3]))
                    column_new.append(convert_to_float(column[1]))
                    column_new.append(convert_to_float(column[2]))
                    column_new.append(convert_to_float(column[4]))
                    column_new.append(convert_to_float(column[5]))
                    column_new.append(float("nan"))
                    column_new.append(convert_to_float(column[6]))
                    for i in range(4):
                        column_new.append(float("nan"))
                    column_new.append(convert_to_float(column[7]))
                    # 如果一行全是nan，则不写入数据
                    print("column_new: ", column_new)
                    table_writer.write(column_new)
        table_writer.close()

    def start_get_chart_5m(self, start_time_dict):
        print("symbols len: ", len(self.symbol_list))
        new_time_dict = {}
        isExists = os.path.exists(self.csv_path)
        if isExists:
            shutil.rmtree(self.csv_path)
            print("folder is deleted...")
        for symbol_code in self.symbol_list:
            symbol_start_time = self.date_to_timestamp(start_time_dict[symbol_code])
            print("get time: ", symbol_code, start_time_dict[symbol_code], symbol_start_time, type(symbol_start_time))
            data = self.get_chart_data(
                currencyPair=symbol_code,
                start=symbol_start_time,
                period=self.KLINE_INTERVAL_5MINUTE)

            self.save_to_csv(data, symbol_code, self.KLINE_INTERVAL_5MINUTE)
            # print("The last line time: ", data[-1]['date'], type(data[-1]['date']), str(data[-1]['date']))
            new_time_dict["Poloniex_" + symbol_code] = data[-1]['date']
            time.sleep(3)
        self.upload_to_odps("5m")
        str_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.backup_binance_csv(str_time)
        print("new_time_dict: ", new_time_dict)
        return new_time_dict


if __name__ == '__main__':
    poloniex_rest = PoloniexRest()
    poloniex_rest.start_get_chart_5m()
    # symbol_list = binance_rest.get_symbols()
    # print(symbol_list)

    # print("symbols len: ", len(poloniex_rest.symbol_list))
    # for symbol_code in poloniex_rest.symbol_list:
    #     data = poloniex_rest.get_chart_data(
    #         currencyPair=symbol_code,
    #         period=poloniex_rest.KLINE_INTERVAL_5MINUTE)
    #     # start_str="1519862400000")
    #     # print(data)
    #     poloniex_rest.save_to_csv(data, symbol_code, poloniex_rest.KLINE_INTERVAL_5MINUTE)
    #     time.sleep(3)
