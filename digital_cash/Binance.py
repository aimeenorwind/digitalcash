import requests
import time
import datetime
import pandas as pd
from digital_cash.common_functions import interval_to_milliseconds, date_to_milliseconds
from Common.odpsClient import OdpsClient
from odps.models import Schema, Column, Partition
import os
import csv
import shutil


class BinanceRest(object):
    # 这是Binance的restful API的基础地址，不用动

    BASE_ENDPOINT = 'https://api.binance.com'
    symbol_list = ["BCCBTC", "BCCUSDT", "BTCUSDT", "EOSBTC", "EOSUSDT", "ETHUSDT", "ETHBTC", "LTCBTC", "LTCUSDT",
                   "XRPBTC", "XRPUSDT"]
    # symbol_list = ["BCCBTC", "BCCUSDT", "BTCUSDT", "EOSBTC", "EOSUSDT", "ETHUSDT", "ETHBTC", "LTCBTC", "LTCUSDT",
    #                "XRPBTC", "XRPUSDT"]

    KLINE_INTERVAL_1MINUTE = '1m'
    # KLINE_INTERVAL_3MINUTE = '3m'
    # KLINE_INTERVAL_5MINUTE = '5m'
    # KLINE_INTERVAL_15MINUTE = '15m'
    # KLINE_INTERVAL_30MINUTE = '30m'
    KLINE_INTERVAL_1HOUR = '1h'
    # KLINE_INTERVAL_2HOUR = '2h'
    # KLINE_INTERVAL_4HOUR = '4h'
    # KLINE_INTERVAL_6HOUR = '6h'
    # KLINE_INTERVAL_8HOUR = '8h'
    # KLINE_INTERVAL_12HOUR = '12h'
    KLINE_INTERVAL_1DAY = '1d'

    # KLINE_INTERVAL_3DAY = '3d'
    # KLINE_INTERVAL_1WEEK = '1w'
    # KLINE_INTERVAL_1MONTH = '1M'
    def __init__(self):
        print("BinanceRest class is started....")
        # self.table_name = "zz_test"
        self.table_name = "quant_digital_cash_kline_increment"
        self.binance_folder = "D:\\WorkSpace\\DownloadData\\DownloadDigitalCash\\Binance\\"
        self.csv_path = self.binance_folder + "latest\\"

    def get_klines(self, symbol, interval, startTime=None, endTime=None, limit=1000):
        # 函数可以获取K线图数据，详见
        # https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md

        url = self.BASE_ENDPOINT + '/api/v1/klines'
        params = {
            'symbol': symbol,  # 交易币对，格式为 BTCUSDT
            'interval': interval,  # 数据的时间周期，可选值有1m、3m、5m、15m、30m、1h、2h、4h、6h、8h、12h、1d、3d、1w、1M
            # m -> minutes; h -> hours; d -> days; w -> weeks; M -> months
            'startTime': startTime,  # 起始时间，时间戳（单位ms）
            'endTime': endTime,  # 结束时间，时间戳（单位ms）
            'limit': limit  # 单次获取信息数量（最大值1000）
        }
        response = requests.get(url, params)
        # print("startTime: ----", startTime, type(startTime))
        timeArray = time.localtime(startTime / 1000)
        otherStyleTime = time.strftime('%Y-%m-%d %H:%M:%S', timeArray)
        # print("otherStyleTime: ", otherStyleTime, response.json())
        print('Getted data from', otherStyleTime, response.url)  # 打印获取数据的具体地址
        # print("response.json() result: ", response.json())
        return response.json()

    def get_symbols(self):
        url = self.BASE_ENDPOINT + '/api/v3/ticker/price'
        response = requests.get(url)
        # print("response: ", response.json())
        # print([res["symbol"] for res in response.json()])
        return [res["symbol"] for res in response.json()]

    def _get_earliest_valid_timestamp(self, symbol, interval):
        """Get earliest valid open timestamp from Binance

        :param symbol: Name of symbol pair e.g BNBBTC
        :type symbol: str
        :param interval: Binance Kline interval
        :type interval: str

        :return: first valid timestamp

        """
        kline = self.get_klines(
            symbol=symbol,
            interval=interval,
            limit=1,
            startTime=0,
            endTime=None
        )
        # print("_get_earliest_valid_timestamp--get_klines: ", kline)
        return kline[0][0]

    def get_historical_klines(self, symbol, interval, start_str, end_str=None):
        """Get Historical Klines from Binance

        See dateparser docs for valid start and end string formats http://dateparser.readthedocs.io/en/latest/

        If using offset strings for dates add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"

        :param symbol: Name of symbol pair e.g BNBBTC
        :type symbol: str
        :param interval: Binance Kline interval
        :type interval: str
        :param start_str: Start date string in UTC format or timestamp in milliseconds
        :type start_str: str|int
        :param end_str: optional - end date string in UTC format or timestamp in milliseconds (default will fetch everything up to now)
        :type end_str: str|int

        :return: list of OHLCV values

        symbol="BNBBTC",
        interval=self.KLINE_INTERVAL_1MINUTE,
        start_str="1st March 2018"

        """
        print("start getting data: get_historical_klines......")
        # init our list
        output_data = []

        # setup the max limit
        limit = 1000

        # convert interval to useful value in seconds
        timeframe = interval_to_milliseconds(interval)

        # convert our date strings to milliseconds
        if type(start_str) == int:
            start_ts = start_str
        else:
            start_ts = date_to_milliseconds(start_str)
        # print("start_str: ", start_str)
        # print("history start_ts: ", start_ts)
        # establish first available start timestamp
        first_valid_ts = self._get_earliest_valid_timestamp(symbol, interval)
        # print("start_ts: ", start_ts)
        # print("first_valid_ts: ", first_valid_ts)
        start_ts = max(start_ts, first_valid_ts)

        # if an end time was passed convert it
        end_ts = None
        if end_str:
            if type(end_str) == int:
                end_ts = end_str
            else:
                end_ts = date_to_milliseconds(end_str)

        idx = 0
        while True:
            # fetch the klines from start_ts up to max 500 entries or the end_ts if set
            temp_data = self.get_klines(
                symbol=symbol,
                interval=interval,
                limit=limit,
                startTime=start_ts,
                endTime=end_ts
            )

            # handle the case where exactly the limit amount of data was returned last loop
            if not len(temp_data):
                break

            # append this loops data to our output data
            output_data += temp_data

            # set our start timestamp using the last value in the array
            start_ts = temp_data[-1][0]

            idx += 1
            # check if we received less than the required limit and exit the loop
            if len(temp_data) < limit:
                # exit the while loop
                break

            # increment next call by our timeframe
            start_ts += timeframe

            # sleep after every 3rd call to be kind to the API
            if idx % 3 == 0:
                time.sleep(1)

        return output_data

    def save_to_csv(self, datalist, symbol, interval):
        isExists = os.path.exists(self.csv_path)
        if not isExists:
            os.makedirs(self.csv_path)
            print("folder is created...")
        for data in datalist:
            timeArray0 = time.localtime(data[0] / 1000)
            data[0] = time.strftime('%Y-%m-%d %H:%M:%S', timeArray0)
            timeArray6 = time.localtime(data[6] / 1000)
            data[6] = time.strftime('%Y-%m-%d %H:%M:%S', timeArray6)
            data.append(interval)
        df = pd.DataFrame(datalist)
        df.columns = ['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'Quote asset volume',
                      'Number of trades', 'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore',
                      'interval']
        # print(df[:3])

        # print("csv file name: ", symbol, datalist[0][0])
        # print("csv file name: ", "Binance_{}.csv".format(symbol))
        file_name = "Binance_{}.csv".format(symbol)
        df.to_csv(self.csv_path + file_name, mode='a', sep=',', header=True, index=None)

    def backup_binance_csv(self, current_time):
        backup_path = self.binance_folder + current_time + "\\"
        source_path = self.csv_path
        # isExists = os.path.exists(backup_path)
        # if not isExists:
        #     os.makedirs(backup_path)
        #     print("backup_path: ", backup_path)

        # shutil.copyfile(self.csv_path+"Binance_BCCBTC.csv", backup_path)
        # shutil.copy(self.csv_path, backup_path)
        # shutil.copytree(source_path, backup_path)
        shutil.move(source_path, backup_path)

    def upload_to_odps(self, interval):
        odps_basic = OdpsClient()
        table_name = odps_basic.get_digital_table(self.table_name)
        partitions = "interval=" + interval
        table_writer = table_name.open_writer(partition=partitions, create_partition=True)

        # 读取csv文件数据写入table
        for parent, dirnames, filenames in os.walk(self.csv_path):
            break
        for filename in filenames:
            print(filename)
            currencyPair = filename[8:].replace(".csv", "")
            partitions = "interval=" + interval
            with open(self.csv_path + filename, "r", encoding="utf-8") as csvfile:
                # 读取csv文件，返回的是迭代类型
                reader = csv.reader(csvfile)
                columns = [row for row in reader]
                # print("---增加记录数量：---", len(columns))
                csvfile.close()
                empty_row = 0
                for column in columns[1:-1]:
                    column_new = ["Binance"]
                    column_new.append(currencyPair)
                    column_new.append(column[0])
                    for col in column[1:6]:
                        if (col == 'None' or col == '' or col == 'nan'):
                            column_new.append(float("nan"))
                        else:
                            column_new.append(float(col))
                    column_new.append(column[6])
                    for col in column[7:12]:
                        if (col == 'None' or col == '' or col == 'nan'):
                            column_new.append(float("nan"))
                        else:
                            column_new.append(float(col))
                    column_new.append(float("nan"))
                    # 如果一行全是nan，则不写入数据
                    # print("column_new: ", column_new)
                    table_writer.write(column_new)
        table_writer.close()

    def date_to_timestamp(self, date, format_string="%Y-%m-%d %H:%M:%S"):
        # print("原始日期：", type(date))
        time_array = time.strptime(date, format_string)
        # print("datetime: ", time_array)
        # time_stamp = int(time.mktime(time_array))
        time_stamp = int(round(time.mktime(time_array) * 1000))
        # print("time stamp: ", time_stamp)
        return time_stamp

    def start_kline_1m(self, start_time_dict):
        # print("start_kline_1m - start_time_dict: ", start_time_dict)
        print("symbols len: ", len(self.symbol_list))
        new_time_dict = {}
        isExists = os.path.exists(self.csv_path)
        if isExists:
            shutil.rmtree(self.csv_path)
            print("folder is deleted...")
        for symbol_code in self.symbol_list:
            symbol_start_time = int(self.date_to_timestamp(start_time_dict[symbol_code]))
            print("get time: ", symbol_code, symbol_start_time, type(symbol_start_time))
            data = self.get_historical_klines(
                symbol=symbol_code,
                interval=self.KLINE_INTERVAL_1MINUTE,
                start_str=symbol_start_time)
            self.save_to_csv(data, symbol_code, self.KLINE_INTERVAL_1MINUTE)
            # print("The last line time: ", data[-1][0], type(data[-1][0]), str(data[-1][0]))
            new_time_dict["Binance_" + symbol_code] = data[-1][0]
        self.upload_to_odps("1m")
        str_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.backup_binance_csv(str_time)
        # print("new_time_dict: ", new_time_dict)
        return new_time_dict


if __name__ == '__main__':
    binance_rest = BinanceRest()
    # symbol_list = binance_rest.get_symbols()
    # print(symbol_list)
    # timestamp_dict = {'BCCBTC': 1536305460000, 'BCCUSDT': 1536297240000, 'BTCUSDT': 1536299220000, 'EOSBTC': 1536299880000, 'EOSUSDT': 1536300060000, 'ETHUSDT': 1536300900000, 'ETHBTC': 1536291540000, 'LTCBTC': 1536302340000, 'LTCUSDT': 1536302940000, 'XRPBTC': 1536303600000, 'XRPUSDT': 1537334640000}
    # timestamp_dict = {'BCCBTC': 1536305460000, 'BCCUSDT': 1536297240000}
    # binance_rest.start_kline_1m(timestamp_dict)
    # binance_rest.upload_to_odps("1m")
    # str_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    # binance_rest.backup_binance_csv(str_time)
    """
    print("symbols len: ", len(binance_rest.symbol_list))
    for symbol_code in binance_rest.symbol_list:
        data = binance_rest.get_historical_klines(
            symbol=symbol_code,
            interval=binance_rest.KLINE_INTERVAL_1MINUTE,
            start_str="1st January 2010")
        # start_str="1519862400000")
        # print(data)
        binance_rest.save_to_csv(data, symbol_code, binance_rest.KLINE_INTERVAL_1MINUTE)
    """
