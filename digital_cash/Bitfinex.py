"""
有请求次数限制10-45/m不等，所以设置每3次请求中间等10s
"""
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


class BitfinexRest(object):
    # 这是Binance的restful API的基础地址，不用动
    BASE_ENDPOINT = 'https://api.bitfinex.com/v2/candles'

    symbol_list = ['BCHBTC', 'BCHUSD', 'BTCUSD', 'DSHUSD', 'EOSBTC', 'EOSUSD', 'ETCUSD', 'ETHBTC', 'ETHUSD', 'ETPUSD',
                   'IOTUSD', 'LTCBTC', 'LTCUSD', 'NEOUSD', 'OMGUSD', 'XLMUSD', 'XMRBTC', 'XRPBTC', 'XRPUSD', 'ZECUSD']

    # symbol_list = ['BCHBTC', 'BCHUSD', 'BTCUSD', 'DSHUSD', 'EOSBTC', 'EOSUSD', 'ETCUSD', 'ETHBTC', 'ETHUSD', 'ETPUSD',
    #                'IOTUSD', 'LTCBTC', 'LTCUSD', 'NEOUSD', 'OMGUSD', 'XLMUSD', 'XMRBTC', 'XRPBTC', 'XRPUSD', 'ZECUSD']

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
        print("BitfinexRest class is started....")
        # self.table_name = "zz_test"
        self.table_name = "quant_digital_cash_kline_increment"
        self.bitfinex_folder = "D:\\WorkSpace\\DownloadData\\DownloadDigitalCash\\Bitfinex\\"
        self.csv_path = self.bitfinex_folder + "latest\\"

    def candles(self, symbol, interval, startTime=None, endTime=None, limit=1000):
        # 函数可以获取K线图数据，详见https://docs.bitfinex.com/v2/reference#rest-public-candles
        url = self.BASE_ENDPOINT + '/trade:{0}:t{1}/hist'.format(interval, symbol)
        # url中可设置获取的交易对币、时间周期。币对格式例：tBTCUSDT
        # 可选择的时间间隔有：'1m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '1D', '7D', '14D', '1M'
        params = {
            'limit': 1000,
            'start': startTime,
            'end': endTime,
            'sort': 1
        }
        # limit表示单次获取数据量，最大限值是1000；start、end为起始结束时间戳，单位为ms
        response = requests.get(url, params)
        timeArray = time.localtime(startTime / 1000)
        otherStyleTime = time.strftime('%Y-%m-%d %H:%M:%S', timeArray)
        print('Getted data from', otherStyleTime, response.url)  # 打印获取数据的具体地址
        print("response.json(): ", response.json())
        return response.json()

    def _get_earliest_valid_timestamp(self, symbol, interval):
        """Get earliest valid open timestamp from Binance

        :param symbol: Name of symbol pair e.g BNBBTC
        :type symbol: str
        :param interval: Binance Kline interval
        :type interval: str

        :return: first valid timestamp

        """
        kline = self.candles(
            symbol=symbol,
            interval=interval,
            limit=1,
            startTime=0,
            endTime=None
        )
        # print("_get_earliest_valid_timestamp--get_klines: ", kline)
        return kline[0][0]

    def get_historical_candles(self, symbol, interval, start_str, end_str=None):
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

        # establish first available start timestamp
        first_valid_ts = self._get_earliest_valid_timestamp(symbol, interval)
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
            temp_data = self.candles(
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
            if idx % 2 == 0:
                time.sleep(10)

        return output_data

    def convert_to_float(self, col):
        if (col == 'None' or col == '' or col == 'nan'):
            return float("nan")
        else:
            return float(col)

    def upload_to_odps(self, interval):
        odps_basic = OdpsClient()
        table_name = odps_basic.get_digital_table(self.table_name)
        partitions = "interval=" + interval
        table_writer = table_name.open_writer(partition=partitions, create_partition=True)

        # 读取csv文件数据写入table
        for parent, dirnames, filenames in os.walk(self.csv_path):
            break
        for filename in filenames:
            print("Upload files to odps: ", filename)
            currencyPair = filename[9:].replace(".csv", "")
            with open(self.csv_path + filename, "r", encoding="utf-8") as csvfile:
                # 读取csv文件，返回的是迭代类型
                reader = csv.reader(csvfile)
                columns = [row for row in reader]
                # print("---增加记录数量：---", len(columns))
                csvfile.close()
                for column in columns[1:-1]:
                    column_new = ["Bitfinex"]
                    column_new.append(currencyPair)
                    column_new.append(column[0])
                    column_new.append(self.convert_to_float(column[1]))
                    column_new.append(self.convert_to_float(column[3]))
                    column_new.append(self.convert_to_float(column[4]))
                    column_new.append(self.convert_to_float(column[2]))
                    column_new.append(self.convert_to_float(column[5]))
                    for i in range(7):
                        column_new.append(float("nan"))

                    # 如果一行全是nan，则不写入数据
                    # print("column_new: ", column_new)
                    table_writer.write(column_new)
        table_writer.close()

    def date_to_timestamp(self, date, format_string="%Y-%m-%d %H:%M:%S"):
        print("原始日期：", date)
        time_array = time.strptime(date, format_string)
        # print("datetime: ", time_array)
        # time_stamp = int(time.mktime(time_array))
        time_stamp = int(round(time.mktime(time_array) * 1000))
        print("time stamp: ", time_stamp)
        return time_stamp

    def save_to_csv(self, datalist, symbol, interval):
        isExists = os.path.exists(self.csv_path)
        if not isExists:
            os.makedirs(self.csv_path)
            print("folder is created...")
        if datalist != [] or datalist[0] != 'error':
            for data in datalist:
                timeArray0 = time.localtime(data[0] / 1000)
                data[0] = time.strftime('%Y-%m-%d %H:%M:%S', timeArray0)
                data.append(interval)
            df = pd.DataFrame(datalist)
            df.columns = ['Open time', 'Open', 'Close', 'High', 'Low', 'Volume', 'interval']
            # print(df[:3])
            file_name = "Bitfinex_{}.csv".format(symbol)
            df.to_csv(self.csv_path + file_name, mode='a', sep=',', header=True, index=None)

    def backup_binance_csv(self, current_time):
        backup_path = self.bitfinex_folder + current_time+"\\"
        source_path = self.csv_path
        shutil.move(source_path, backup_path)

    def start_candle_1m(self, start_time_dict):
        print("symbols len: ", len(self.symbol_list))
        new_time_dict = {}
        isExists = os.path.exists(self.csv_path)
        if isExists:
            shutil.rmtree(self.csv_path)
            print("folder is deleted...")
        for symbol_code in self.symbol_list:
            time.sleep(8)
            symbol_start_time = int(self.date_to_timestamp(start_time_dict[symbol_code]))
            # print("Time: ===", symbol_start_time)
            bitfinex_data = self.get_historical_candles(
                symbol=symbol_code,
                interval=self.KLINE_INTERVAL_1MINUTE,
                start_str=symbol_start_time)
            self.save_to_csv(bitfinex_data, symbol_code, self.KLINE_INTERVAL_1MINUTE)
            new_time_dict["Bitfinex_" + symbol_code] = bitfinex_data[-1][0]

        self.upload_to_odps("1m")
        str_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.backup_binance_csv(str_time)
        print("new_time_dict: ", new_time_dict)
        return new_time_dict


if __name__ == '__main__':
    bitfinex_rest = BitfinexRest()
    bitfinex_dc_dict = {'BCHBTC': '2018-09-07 16:56:00', 'BCHUSD': '2018-09-07 17:36:00', 'BTCUSD': '2018-09-10 10:58:00', 'DSHUSD': '2018-09-10 11:31:00', 'EOSBTC': '2018-09-10 12:04:00', 'EOSUSD': '2018-09-10 12:48:00', 'ETCUSD': '2018-09-10 14:02:00', 'ETHBTC': '2018-09-10 14:57:00', 'ETHUSD': '2018-09-10 16:24:00', 'ETPUSD': '2018-09-10 16:43:00', 'IOTUSD': '2018-09-10 17:26:00', 'LTCBTC': '2018-09-10 18:00:00', 'LTCUSD': '2018-09-10 19:01:00', 'NEOUSD': '2018-09-10 19:30:00', 'OMGUSD': '2018-09-10 20:05:00', 'XLMUSD': '2018-09-10 20:02:00', 'XMRBTC': '2018-09-10 20:28:00', 'XRPBTC': '2018-09-10 21:05:00', 'XRPUSD': '2018-09-10 21:50:00', 'ZECUSD': '2018-09-10 22:21:00'}

    bitfinex_rest.start_candle_1m(bitfinex_dc_dict)
    # symbol_list = binance_rest.get_symbols()
    # print(symbol_list)
    """
    print("symbols len: ", len(bitfinex_rest.symbol_list))
    for symbol_code in bitfinex_rest.symbol_list:
        data = bitfinex_rest.get_historical_candles(
            symbol=symbol_code,
            interval=bitfinex_rest.KLINE_INTERVAL_1MINUTE,
            start_str="1st January 2017")
        # start_str="1519862400000")
        # print(data)
        bitfinex_rest.save_to_csv(data, symbol_code, bitfinex_rest.KLINE_INTERVAL_1MINUTE)
    """