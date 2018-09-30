import redis


class DigitalCashTime():
    def __init__(self):
        try:
            # host is the redis_test_only host,the redis_test_only server and client are required to open, and the redis_test_only default port is 6379
            pool = redis.ConnectionPool(host='localhost', password='123456', port=6379, decode_responses=True)
            print("connected success.")
        except Exception as e:
            print("could not connect to redis.")
        self.r = redis.Redis(connection_pool=pool)

    def set_binance_time(self, timestamp_dict):
        print("Update redis starting....")
        self.r.hmset("Binance", timestamp_dict)
        print("Update redis finished....")

    def get_binance_time(self, digitalcash_list):
        # print("start get timestamps: get_binance_time", digitalcash_list)
        cash_time_list = self.r.hmget("Binance", digitalcash_list)
        cash_time_list_int = [int(i) for i in cash_time_list]
        # print("cash_time_list: ", cash_time_list_int)
        return dict(zip(digitalcash_list, cash_time_list_int))

    def set_redis_time(self, platform, timestamp_dict):
        """
        根据最新的下载数据的时间更新redis中时间
        :param platform: 如："Binance", "Bitfinex"
        :param timestamp_dict: {'BCHBTC': '2018-09-07 16:56:00', 'BCHUSD': '2018-09-07 17:36:00', ...}
        :return:
        """
        print("Update redis starting....")
        self.r.hmset(platform, timestamp_dict)
        print("Update redis finished....")

    def get_redis_time(self, platform, digitalcash_list):
        """
        获取redis中的时间，便于下载更新的数据
        :param platform: 如："Binance", "Bitfinex"
        :param digitalcash_list: ['BCCBTC', 'BCCUSDT', 'BTCUSDT', ...]
        :return:
        """
        # print("start get timestamps: get_binance_time", digitalcash_list)
        symbol_list_platform = [platform + "_" + i for i in digitalcash_list]
        cash_time_list = self.r.hmget(platform, symbol_list_platform)
        # cash_time_list_int = [int(i) for i in cash_time_list]
        print("cash_time_dict: ", dict(zip(digitalcash_list, cash_time_list)))
        return dict(zip(digitalcash_list, cash_time_list))


if __name__ == '__main__':
    redis_digital_cash_time = DigitalCashTime()
    digitalcash_list = ['BCCBTC', 'BCCUSDT', 'BTCUSDT', 'EOSBTC', 'EOSUSDT', 'ETHUSDT', 'ETHBTC', 'LTCBTC', 'LTCUSDT',
                        'XRPBTC', 'XRPUSDT']
    redis_digital_cash_time.get_binance_time(digitalcash_list)
