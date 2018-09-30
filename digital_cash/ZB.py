"""
只能取当前时间往前最多1000条记录
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

class ZBRest(object):
    # 这是Binance的restful API的基础地址，不用动
    BASE_ENDPOINT = 'http://api.zb.cn/data/v1'
    symbol_list = ['ada_btc', 'ada_usdt', 'bch_btc', 'bch_usdt', 'btc_usdt', 'dash_btc', 'dash_usdt', 'eos_btc',
                   'eos_usdt', 'etc_btc', 'etc_usdt', 'eth_btc', 'eth_usdt', 'ltc_btc', 'ltc_usdt', 'qtum_btc',
                   'qtum_usdt', 'xrp_btc', 'xrp_usdt', 'zbb_tc', 'zbu_sdt']
    # symbol_list = ['ada_btc', 'ada_usdt', 'bch_btc', 'bch_usdt', 'btc_usdt', 'dash_btc', 'dash_usdt', 'eos_btc',
    #                'eos_usdt', 'etc_btc', 'etc_usdt', 'eth_btc', 'eth_usdt', 'ltc_btc', 'ltc_usdt', 'qtum_btc',
    #                'qtum_usdt', 'xrp_btc', 'xrp_usdt', 'zbb_tc', 'zbu_sdt']

    KLINE_INTERVAL_1MINUTE = '1min'
    KLINE_INTERVAL_3MINUTE = '3min'
    KLINE_INTERVAL_5MINUTE = '5min'
    KLINE_INTERVAL_15MINUTE = '15min'
    KLINE_INTERVAL_30MINUTE = '30min'
    KLINE_INTERVAL_1HOUR = '1hour'
    KLINE_INTERVAL_2HOUR = '2hour'
    KLINE_INTERVAL_4HOUR = '4hour'
    KLINE_INTERVAL_6HOUR = '6hour'
    KLINE_INTERVAL_12HOUR = '12hour'
    KLINE_INTERVAL_1DAY = '1day'
    KLINE_INTERVAL_3DAY = '3day'
    KLINE_INTERVAL_1WEEK = '1week'

    def __init__(self):
        print("ZBRest class is started....")
        # self.table_name = "zz_test"
        self.table_name = "quant_digital_cash_kline_increment"
        self.ZB_folder = "D:\\WorkSpace\\DownloadData\\DownloadDigitalCash\\ZB\\"
        self.csv_path_latest = self.ZB_folder + "latest\\"
        self.csv_path_previous = self.ZB_folder + "previous\\"

    def get_klines(self, market, interval, since, limit=1000):
        # 函数可以获取K线图数据，详见https://www.zb.cn/i/developer

        url = self.BASE_ENDPOINT + '/kline'
        params = {'market': market,
                  'type': interval,
                  'since': since,
                  'size': limit}
        response = requests.get(url, params)
        # print("startTime: ----", startTime, type(startTime))
        timeArray = time.localtime(since / 1000)
        otherStyleTime = time.strftime('%Y-%m-%d %H:%M:%S', timeArray)
        # print("otherStyleTime: ", otherStyleTime, response.json())
        print('Getted data from', otherStyleTime, response.url)  # 打印获取数据的具体地址
        # print("response.json() result: ", response.json())
        return response.json()

    def save_to_csv(self, datalist, symbol, interval):
        isExists = os.path.exists(self.csv_path_latest)
        if not isExists:
            os.makedirs(self.csv_path_latest)
            print("folder is created...")
        for data in datalist:
            timeArray0 = time.localtime(data[0] / 1000)
            data[0] = time.strftime('%Y-%m-%d %H:%M:%S', timeArray0)
            data.append(interval)
        df = pd.DataFrame(datalist)
        df.columns = ['Timestamp', 'Opening Price', 'High Price', 'Low Price', 'Closing Price', 'Volume', 'interval']
        # print(df[:3])
        file_name = "ZB_{}.csv".format(symbol)
        df.to_csv(self.csv_path_latest + file_name, mode='a', sep=',', header=True, index=None)

    def upload_to_odps(self, interval, previous_path, latest_path):
        odps_basic = OdpsClient()
        table_name = odps_basic.get_digital_table(self.table_name)
        partitions = "interval=" + interval
        table_writer = table_name.open_writer(partition=partitions, create_partition=True)

        dc_dict = {}
        for parent, dirnames, filenames in os.walk(previous_path):
            break
        for filename in filenames:
            # print("********", filename)
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
            currencyPair = (filename[3:].replace(".csv", "").replace("_", "")).upper()
            partitions = "interval=" + interval
            table_writer = table_name.open_writer(partition=partitions, create_partition=True)

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
                print("start_row: ", filename, start_row)
                for column in columns[start_row + 1:]:
                    column_new = ["ZB"]
                    column_new.append(currencyPair)
                    column_new.append(column[0])
                    column_new.append(convert_to_float(column[3]))
                    column_new.append(convert_to_float(column[1]))
                    column_new.append(convert_to_float(column[2]))
                    column_new.append(convert_to_float(column[4]))
                    column_new.append(convert_to_float(column[5]))
                    for i in range(7):
                        column_new.append(float("nan"))
                    # 如果一行全是nan，则不写入数据
                    # print("column_new: ", column_new)
                    table_writer.write(column_new)
        table_writer.close()

    def backup_HitBTC_csv(self, previous_path, latest_path):
        str_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        isExists = os.path.exists(previous_path)
        if isExists:
            os.rename(previous_path, self.ZB_folder+str_time+"\\")
        os.rename(latest_path, previous_path)

    def start_get_kline_1m(self):
        isExists = os.path.exists(self.csv_path_latest)
        if isExists:
            shutil.rmtree(self.csv_path_latest)
            print("folder is deleted...")
        print("symbols len: ", len(self.symbol_list))
        for symbol_code in self.symbol_list:
            data = self.get_klines(
                market=symbol_code,
                interval=self.KLINE_INTERVAL_1MINUTE,
                since=1199116800000)
            # start_str="1519862400000")
            # print(data)
            self.save_to_csv(data['data'], symbol_code, self.KLINE_INTERVAL_1MINUTE)
            time.sleep(5)
        self.upload_to_odps("1m", self.csv_path_previous, self.csv_path_latest)
        self.backup_HitBTC_csv(self.csv_path_previous, self.csv_path_latest)


if __name__ == '__main__':
    zb_rest = ZBRest()
    zb_rest.start_get_kline_1m()
    # symbol_list = binance_rest.get_symbols()
    # print(symbol_list)

    # print("symbols len: ", len(zb_rest.symbol_list))
    # for symbol_code in zb_rest.symbol_list:
    #     data = zb_rest.get_klines(
    #         market=symbol_code,
    #         interval=zb_rest.KLINE_INTERVAL_1MINUTE,
    #         since=1199116800000)
    #     # start_str="1519862400000")
    #     # print(data)
    #     zb_rest.save_to_csv(data['data'], symbol_code, zb_rest.KLINE_INTERVAL_1MINUTE)
    #     time.sleep(5)
