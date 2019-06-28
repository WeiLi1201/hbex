# -*- coding: utf-8 -*-

from trade import trade_plan
import time
from logger import hbtrade_logger as log
from apscheduler.schedulers.blocking import BlockingScheduler
# from apscheduler.schedulers.background import BackgroundScheduler

if __name__ == '__main__':
    trade_plan.generate_trade_plan()
    sched = BlockingScheduler()
    sched.add_job(trade_plan.generate_trade_plan, 'interval', seconds=20, max_instances=10)
    sched.start()






