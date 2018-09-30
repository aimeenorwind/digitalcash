from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import datetime
import logging
from Schedule.time_update_redis import DigitalCashTime
from digital_cash.Binance import BinanceRest
from digital_cash.Bitfinex import BitfinexRest
from digital_cash.HitBTC import HitBTCRest
from digital_cash.Houbi import HoubiRest
from digital_cash.Poloniex import PoloniexRest
from digital_cash.ZB import ZBRest


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S",
                    filename="log1.txt",
                    filemode="a")

def get_binance_data():
    print("start job: get_binance_data")
    dc_time = DigitalCashTime()
    binance_rest = BinanceRest()
    start_time_dict = dc_time.get_redis_time("Binance", binance_rest.symbol_list)
    print("start_time_dict: ", start_time_dict)
    timestamp_dict = binance_rest.start_kline_1m(start_time_dict)
    dc_time.set_redis_time("Binance", timestamp_dict)

def get_bitfinex_data():
    # print("start job: get_bitfinex_data ...")
    dc_time = DigitalCashTime()
    bitfinex_rest = BitfinexRest()
    start_time_dict = dc_time.get_redis_time("Bitfinex", bitfinex_rest.symbol_list)
    print("start_time_dict: ", start_time_dict)
    timestamp_dict = bitfinex_rest.start_candle_1m(start_time_dict)
    # timestamp_dict = {'Bitfinex_XRPBTC': '2018-09-25 15:58:00', 'Bitfinex_XRPUSD': '2018-09-25 15:58:00',
    #  'Bitfinex_ZECUSD': '2018-09-25 15:55:00'}

    print("The latest timestamp: ", timestamp_dict)
    dc_time.set_redis_time("Bitfinex", timestamp_dict)

def get_HitBTC_data():
    # print("start job: get_bitfinex_data ...")
    hitBTC_rest = HitBTCRest()
    hitBTC_rest.start_candles_1m()

def get_Houbi_data():
    # print("start job: get_bitfinex_data ...")
    houbi_rest = HoubiRest()
    houbi_rest.start_kline_1m()

def get_poloniex_data():
    # print("start job: get_bitfinex_data ...")
    dc_time = DigitalCashTime()
    poloniex_rest = PoloniexRest()
    start_time_dict = dc_time.get_redis_time("Poloniex", poloniex_rest.symbol_list)
    print("start_time_dict: ", start_time_dict)
    timestamp_dict = poloniex_rest.start_get_chart_5m(start_time_dict)
    print("The latest timestamp: ", timestamp_dict)
    dc_time.set_redis_time("Poloniex", timestamp_dict)

def get_ZB_data():
    print("start job: get_zb_data ...")
    zb_rest = ZBRest()
    zb_rest.start_get_kline_1m()


def my_listener(event):
    if event.exception:
        print('The job crashed :(')
        # 发邮件通知
        # event_email = MailSend()
        # event_email.send_mail("邮件的内容是什么，怎么传过来，还要想一想")
    else:
        print('The job worked :)')

# scheduler = BackgroundScheduler()
scheduler = BlockingScheduler()
# 增加job
# scheduler.add_job(func=get_basic_info, trigger="cron", day_of_week='tue-sat', hour=20, minute=00, id="get_basic_info_task")
scheduler.add_job(func=get_binance_data, trigger="interval", hours=7, minutes=10, id="interval_binance_task")
scheduler.add_job(func=get_bitfinex_data, trigger="interval", hours=9, minutes=10, id="interval_bitfinex_task")
scheduler.add_job(func=get_HitBTC_data, trigger="interval", hours=5, minutes=10, id="interval_hitBTC_task")
scheduler.add_job(func=get_Houbi_data, trigger="interval", hours=5, minutes=20, id="interval_houbi_task")
scheduler.add_job(func=get_poloniex_data, trigger="interval",  hours=6, minutes=11, id="interval_poloniex_task")
scheduler.add_job(func=get_ZB_data, trigger="interval", hours=5, minutes=35, id="interval_zb_task")

print("scheduler is defined...")
scheduler._logger = logging
scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
try:
    scheduler.start()
    print("scheduler is started...")
except (KeyboardInterrupt, SystemExit):
    scheduler.shutdown()
