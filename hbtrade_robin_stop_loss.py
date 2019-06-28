# -*- coding: utf-8 -*-

from trade import stop_loss
import time
from logger import hbtrade_logger as log
from apscheduler.schedulers.blocking import BlockingScheduler

if __name__ == '__main__':
    stop_loss.robin_stop_loss()
    sched = BlockingScheduler()
    sched.add_job(stop_loss.robin_stop_loss, 'interval', seconds=30, max_instances=10)
    sched.start()






