"""
没有起始时间，取最近的limit的值，最多2000
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

class HoubiRest(object):
    # 这是Binance的restful API的基础地址，不用动
    BASE_ENDPOINT = 'https://api.huobi.pro'
    symbol_list = ['bchbtc', 'bchusdt', 'btcusdt', 'eosbtc', 'eosusdt', 'ethbtc', 'ethusdt', 'ltcbtc', 'ltcusdt',
                   'xrpbtc', 'xrpusdt']
    # symbol_list = ['bchbtc', 'bchusdt', 'btcusdt', 'eosbtc', 'eosusdt', 'ethbtc', 'ethusdt', 'ltcbtc', 'ltcusdt',
    #                'xrpbtc', 'xrpusdt']

    KLINE_INTERVAL_1MINUTE = '1min'
    # KLINE_INTERVAL_5MINUTE = '5min'
    # KLINE_INTERVAL_15MINUTE = '15min'
    # KLINE_INTERVAL_30MINUTE = '30min'
    KLINE_INTERVAL_1HOUR = '60min'
    # KLINE_INTERVAL_2HOUR = '2h'
    # KLINE_INTERVAL_4HOUR = '4h'
    # KLINE_INTERVAL_6HOUR = '6h'
    # KLINE_INTERVAL_8HOUR = '8h'
    # KLINE_INTERVAL_12HOUR = '12h'
    KLINE_INTERVAL_1DAY = '1d'

    # KLINE_INTERVAL_1WEEK = '1week'
    # KLINE_INTERVAL_1MONTH = '1Mon'
    # KLINE_INTERVAL_1YEAR = '1year'

    def __init__(self):
        print("HoubiRest class is started....")
        # self.table_name = "zz_test"
        self.table_name = "quant_digital_cash_kline_increment"
        self.Houbi_folder = "D:\\WorkSpace\\DownloadData\\DownloadDigitalCash\\Houbi\\"
        self.csv_path_latest = self.Houbi_folder + "latest\\"
        self.csv_path_previous = self.Houbi_folder + "previous\\"

    def get_kline(self, symbol, period, size=1000):
        # 函数可以获取K线图数据，详见
        # https://github.com/huobiapi/API_Docs/wiki/REST_api_reference#get-markethistorykline-获取K线数据

        url = self.BASE_ENDPOINT + '/market/history/kline'
        params = {'symbol': symbol,
                  'period': period,
                  'size': size}
        response = requests.get(url, params)
        # print("otherStyleTime: ", otherStyleTime, response.json())
        print('Getted data from', response.url)  # 打印获取数据的具体地址
        print("response.json() result: ", response.json())
        return response.json()


    def save_to_csv(self, datalist, symbol, interval):
        isExists = os.path.exists(self.csv_path_latest)
        if not isExists:
            os.makedirs(self.csv_path_latest)
            print("folder is created...")
        for data in datalist['data']:
            timeArray = time.localtime(data['id'])
            data['id'] = time.strftime('%Y-%m-%d %H:%M:%S', timeArray)
            data['interval'] = interval
        df = pd.DataFrame(datalist['data'])
        df = df.reindex(columns=['id', 'open', 'close', 'high', 'low', 'amount', 'vol', 'count', 'interval'])
        # print(df[:3])
        file_name = "Houbi_{}.csv".format(symbol)
        df.to_csv(self.csv_path_latest + file_name, mode='a', sep=',', header=True, index=None)

    def backup_HitBTC_csv(self, previous_path, latest_path):
        str_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        isExists = os.path.exists(previous_path)
        if isExists:
            os.rename(previous_path, self.Houbi_folder+str_time+"\\")
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
                time_stamp = columns[1][0]
                dc_dict[filename.replace(".csv", "")] = time_stamp
        print("type time_stamp:===", type(time_stamp))
        print("source file dict: ", dc_dict)

        for parent, dirnames, filenames in os.walk(latest_path):
            break
        for filename in filenames:
            print(filename)
            currencyPair = filename[6:].replace(".csv", "")

            # 读取csv文件数据写入table
            with open(latest_path + filename, "r", encoding="utf-8") as csvfile:
                # 读取csv文件，返回的是迭代类型
                reader = csv.reader(csvfile)
                columns = [row for row in reader]
                # print("---增加记录数量：---", len(columns))
                csvfile.close()
                start_row = len(columns)
                for i in range(1, len(columns)):
                    if dc_dict[filename.replace(".csv", "")] == columns[i][0]:
                        start_row = i
                print("start_row: ", start_row)
                for column in columns[1:start_row]:
                    column_new = ["Houbi"]
                    column_new.append(currencyPair)
                    column_new.append(column[0])
                    column_new.append(convert_to_float(column[1]))
                    column_new.append(convert_to_float(column[3]))
                    column_new.append(convert_to_float(column[4]))
                    column_new.append(convert_to_float(column[2]))
                    column_new.append(convert_to_float(column[5]))
                    column_new.append(float("nan"))
                    column_new.append(convert_to_float(column[6]))
                    column_new.append(convert_to_float(column[7]))
                    for i in range(4):
                        column_new.append(float("nan"))

                    # 如果一行全是nan，则不写入数据
                    print("column_new: ", column_new)
                    table_writer.write(column_new)
        table_writer.close()


    def start_kline_1m(self):
        isExists = os.path.exists(self.csv_path_latest)
        if isExists:
            shutil.rmtree(self.csv_path_latest)
            print("folder is deleted...")
        print("symbols len: ", len(self.symbol_list))
        for symbol_code in self.symbol_list:
            data = self.get_kline(
                symbol=symbol_code,
                period=self.KLINE_INTERVAL_1MINUTE,
                size=2000)
            self.save_to_csv(data, symbol_code, self.KLINE_INTERVAL_1MINUTE)
            time.sleep(3)
        self.upload_to_odps("1m", self.csv_path_previous, self.csv_path_latest)
        self.backup_HitBTC_csv(self.csv_path_previous, self.csv_path_latest)



if __name__ == '__main__':
    houbi_rest = HoubiRest()
    houbi_rest.start_kline_1m()
    # symbol_list = binance_rest.get_symbols()
    # print(symbol_list)
    """
    print("symbols len: ", len(houbi_rest.symbol_list))
    for symbol_code in houbi_rest.symbol_list:
        data = houbi_rest.get_kline(
            symbol=symbol_code,
            period=houbi_rest.KLINE_INTERVAL_1MINUTE,
            size=2000)
        # start_str="1519862400000")
        # print(data)
        houbi_rest.save_to_csv(data, symbol_code, houbi_rest.KLINE_INTERVAL_1MINUTE)
    """
