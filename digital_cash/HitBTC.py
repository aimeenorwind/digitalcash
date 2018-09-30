"""
只能获取最近1000条信息
"""
import requests
import time
import datetime
import pandas as pd
from digital_cash.common_functions import interval_to_milliseconds, date_to_milliseconds, convert_to_float
from Common.odpsClient import OdpsClient
from odps.models import Schema, Column, Partition
import os
import csv
import shutil


class HitBTCRest(object):
    # 这是restful API的基础地址，不用动
    BASE_ENDPOINT = 'https://api.hitbtc.com/api/2/public'

    symbol_list = ['bchbtc', 'bchusd', 'bcnbtc', 'btcusd', 'btgbtc', 'dashbtc', 'dashusd', 'eosusd', 'etcbtc',
                   'etcusd', 'ethbtc', 'ethusd', 'ltcusd', 'neousd', 'trxusd', 'xembtc', 'xmrbtc', 'xmrusd',
                   'xrpbtc', 'zecbtc', 'zecusd']
    # symbol_list = ['bchbtc', 'bchusd', 'bcnbtc', 'btcusd', 'btgbtc', 'dashbtc', 'dashusd', 'eosusd', 'etcbtc',
    #                'etcusd', 'ethbtc', 'ethusd', 'ltcusd', 'neousd', 'trxusd', 'xembtc', 'xmrbtc', 'xmrusd',
    #                'xrpbtc', 'zecbtc', 'zecusd']

    KLINE_INTERVAL_1MINUTE = 'M1'
    # KLINE_INTERVAL_3MINUTE = 'M3'
    # KLINE_INTERVAL_5MINUTE = 'M5'
    # KLINE_INTERVAL_15MINUTE = 'M15'
    # KLINE_INTERVAL_30MINUTE = 'M30'
    KLINE_INTERVAL_1HOUR = 'H1'
    # KLINE_INTERVAL_4HOUR = 'H4'
    KLINE_INTERVAL_1DAY = 'D1'

    # KLINE_INTERVAL_3DAY = 'D7'
    # KLINE_INTERVAL_1MONTH = '1M'

    def __init__(self):
        print("HitBTCRest class is started....")
        # self.table_name = "zz_test"
        self.table_name = "quant_digital_cash_kline_increment"
        self.HitBTC_folder = "D:\\WorkSpace\\DownloadData\\DownloadDigitalCash\\HitBTC\\"
        self.csv_path_latest = self.HitBTC_folder + "latest\\"
        self.csv_path_previous = self.HitBTC_folder + "previous\\"

    def get_candles(self, symbol, interval, limit=1000):
        # 函数可以获取K线图数据，详见https://api.hitbtc.com/?python#candles

        url = self.BASE_ENDPOINT + '/candles/{}'.format(symbol)
        # params = {'limit': 1000, 'period': 'H1'}
        params = {
            'period': interval,
            # limit为单次获取信息数量（最大值1000），默认值为100
            # 时间周期可选择：M1 (1 minute), M3, M5, M15, M30, H1, H4, D1, D7, 1M (1 month)，默认值是 M30 (30 minutes)。
            'limit': limit  # 单次获取信息数量（最大值1000）
        }
        response = requests.get(url, params)
        # print("startTime: ----", startTime, type(startTime))
        # timeArray = time.localtime(startTime / 1000)
        # otherStyleTime = time.strftime('%Y-%m-%d %H:%M:%S', timeArray)
        # print("otherStyleTime: ", otherStyleTime, response.json())
        print('Getted data from', response.url)  # 打印获取数据的具体地址
        # print("response.json() result: ", response.json())
        return response.json()

    def save_to_csv(self, datalist, symbol, interval):
        isExists = os.path.exists(self.csv_path_latest)
        if not isExists:
            os.makedirs(self.csv_path_latest)
            print("folder is created...")
        for data in datalist:
            # timeArray0 = time.localtime(data[0] / 1000)
            # data[0] = time.strftime('%Y-%m-%d %H:%M:%S', timeArray0)
            data['interval'] = interval
        df = pd.DataFrame(datalist,
                          columns=['timestamp', 'open', 'close', 'max', 'min', 'volume', 'volumeQuote', 'interval'])
        # df.columns = ['timestamp', 'open', 'close', 'max', 'min', 'volume', 'volumeQuote']
        file_name = "HitBTC_{}.csv".format(symbol)
        df.to_csv(self.csv_path_latest + file_name, mode='a', sep=',', header=True, index=None)

    def backup_HitBTC_csv(self, previous_path, latest_path):
        str_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        isExists = os.path.exists(previous_path)
        if isExists:
            os.rename(previous_path, self.HitBTC_folder+str_time+"\\")
            # shutil.rmtree(previous_path, self.HitBTC_folder+str_time)
        # os.makedirs(previous_path)
        # shutil.copy(latest_path, previous_path)
        os.rename(latest_path, previous_path)

    def upload_to_odps(self, interval, previous_path, latest_path):
        odps_basic = OdpsClient()
        table_name = odps_basic.get_digital_table(self.table_name)
        partitions = "interval=" + interval
        table_writer = table_name.open_writer(partition=partitions, create_partition=True)

        dc_dict = {}
        for parent, dirnames, filenames in os.walk(previous_path):
            break
        for filename in filenames:
            print("********", filename)
            with open(previous_path + filename, "r", encoding="utf-8-sig") as csvfile:
                # 读取csv文件，返回的是迭代类型
                reader = csv.reader(csvfile)
                columns = [row for row in reader]
                # time_stamp = date_to_timestamp(columns[-1][0])
                time_stamp = columns[-1][0]
                dc_dict[filename.replace(".csv", "")] = time_stamp
        print("type time_stamp:===", type(time_stamp))
        print("source file dict: ", dc_dict)

        for parent, dirnames, filenames in os.walk(latest_path):
            break
        for filename in filenames:
            print(filename)
            currencyPair = filename[7:].replace(".csv", "")

            # 读取csv文件数据写入table
            with open(latest_path + filename, "r", encoding="utf-8") as csvfile:
                # 读取csv文件，返回的是迭代类型
                reader = csv.reader(csvfile)
                columns = [row for row in reader]
                # print("---增加记录数量：---", len(columns))
                csvfile.close()
                start_row = 0
                for i in range(1, len(columns)):
                    if dc_dict[filename.replace(".csv", "")] == columns[i][0]:
                        start_row = i
                        break
                print("start_row: ", start_row)
                for column in columns[start_row + 1:]:
                    column_new = ["HitBTC"]
                    column_new.append(currencyPair)
                    column_new.append(column[0])
                    column_new.append(convert_to_float(column[1]))
                    column_new.append(convert_to_float(column[3]))
                    column_new.append(convert_to_float(column[4]))
                    column_new.append(convert_to_float(column[2]))
                    column_new.append(convert_to_float(column[5]))
                    column_new.append(float("nan"))
                    column_new.append(convert_to_float(column[6]))
                    for i in range(5):
                        column_new.append(float("nan"))

                    # 如果一行全是nan，则不写入数据
                    # print("column_new: ", column_new)
                    table_writer.write(column_new)
        table_writer.close()

    def start_candles_1m(self):
        isExists = os.path.exists(self.csv_path_latest)
        if isExists:
            shutil.rmtree(self.csv_path_latest)
            print("folder is deleted...")
        print("symbols len: ", len(self.symbol_list))
        for symbol_code in self.symbol_list:
            print("symbol_code: ", symbol_code)
            hitbtc_data = self.get_candles(
                symbol=symbol_code,
                interval=self.KLINE_INTERVAL_1MINUTE)
            print(hitbtc_data)
            self.save_to_csv(hitbtc_data, symbol_code, self.KLINE_INTERVAL_1MINUTE)
            time.sleep(5)
        self.upload_to_odps("1m", self.csv_path_previous, self.csv_path_latest)
        self.backup_HitBTC_csv(self.csv_path_previous, self.csv_path_latest)


if __name__ == '__main__':
    hitBTC_rest = HitBTCRest()
    # symbol_list = binance_rest.get_symbols()
    # print(symbol_list)
    hitBTC_rest.start_candles_1m()
    # hitBTC_rest.upload_to_odps("1m", hitBTC_rest.csv_path_previous, hitBTC_rest.csv_path_latest)
    # hitBTC_rest.backup_HitBTC_csv(hitBTC_rest.csv_path_previous, hitBTC_rest.csv_path_latest)
    """
    print("symbols len: ", len(hitBTC_rest.symbol_list))
    for symbol_code in hitBTC_rest.symbol_list:
        data = hitBTC_rest.get_candles(
            symbol=symbol_code,
            interval=hitBTC_rest.KLINE_INTERVAL_1MINUTE)
        print(data)
        hitBTC_rest.save_to_csv(data, symbol_code, hitBTC_rest.KLINE_INTERVAL_1MINUTE)
        time.sleep(5)
        """
