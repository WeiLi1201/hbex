# -*- coding: utf-8 -*-

from tradetool.hb_trade import HBTrade
import time
from logger import hbtrade_logger as log
from apscheduler.schedulers.blocking import BlockingScheduler
# from apscheduler.schedulers.background import BackgroundScheduler

if __name__ == '__main__':
    hbtrade = HBTrade()
    acct_id = hbtrade.ACCOUNT_ID


    # 基准价格，昨日收盘价
    hbtrade.set_base_trade_price()
    # 交易对价格/数量 精度
    hbtrade.set_symbols_precision()

    time.sleep(10)
    # 账户实时持仓
    hbtrade.set_rt_account_holding(acct_id)
    # 实时价格存库 47usdt交易对
    hbtrade.set_rt_price_to_mongo()
    hbtrade.set_currency_back_list()

    sched = BlockingScheduler()
    sched.add_job(hbtrade.set_rt_account_holding, 'interval', seconds=30, max_instances=10, args=[acct_id])
    sched.add_job(hbtrade.set_rt_price_to_mongo, 'interval', minutes=2, max_instances=10)
    sched.add_job(hbtrade.set_currency_back_list, 'interval', minutes=1, max_instances=10)
    sched.add_job(hbtrade.set_base_trade_price, 'interval', minutes=30)
    sched.add_job(hbtrade.set_symbols_precision, 'interval', minutes=40)
    sched.add_job(hbtrade.del_black_list, 'cron', hour=0, minute=0, second=0)

    sched.start()






