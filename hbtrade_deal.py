# -*- coding: utf-8 -*-

from trade import trade_deal
import time
from logger import hbtrade_logger as log
from apscheduler.schedulers.blocking import BlockingScheduler

if __name__ == '__main__':
    trade_deal.make_trade_plan_deal()
    sched = BlockingScheduler()
    sched.add_job(trade_deal.make_trade_plan_deal, 'interval', seconds=30, max_instances=10)
    sched.start()






