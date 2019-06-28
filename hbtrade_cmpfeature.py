# -*- coding: utf-8 -*-

from strategy import compute_feature as cmp
import datetime
from logger import hbtrade_logger as log
from apscheduler.schedulers.blocking import BlockingScheduler


def good():
    print(datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))


if __name__ == '__main__':
    # cmp.cmp_features()
    sched = BlockingScheduler()
    sched.add_job(cmp.cmp_features, 'cron', minute='1', hour='*')
    sched.start()
