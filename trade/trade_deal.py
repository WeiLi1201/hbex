#  -*- coding: utf-8 -*-

import pandas as pd
from pymongo import MongoClient
import datetime
from tradetool.make_deal import MakeDeal
from huobi.HuobiServices import HuobiApi
import time
from logger import tradesignal_logger as log
from tradetool.hb_trade import HBTrade

hbapi = HuobiApi()
# DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['test']
deal = MakeDeal()
hbtrade = HBTrade()


def get_trade_plan(interval=2):
    log.info('尝试获取交易计划, ts: %s' % datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
    trade_plan = hbtrade.DB_CONN.get_collection('trade_plan').find_one({"ts": {'$lt': datetime.datetime.utcnow(),
                                                                       '$gte': datetime.datetime.utcnow() - datetime.timedelta(
                                                                           minutes=interval)}, "deal": False},
                                                               {"keep": 1, "buy": 1, "sell": 1},
                                                               sort=[('ts', -1)], limit=1)
    if trade_plan is not None:
        keep = pd.DataFrame(data=trade_plan['keep'], columns=['symbol', 'balance'])
        buy = pd.DataFrame(data=trade_plan['buy'], columns=['symbol', 'balance', 'buy_sum'])
        sell = pd.DataFrame(data=trade_plan['sell'], columns=['symbol', 'balance'])

        if len(buy) + len(keep) > 9:
            buy_pairs = 9 - len(keep)
            buy = buy.head(buy_pairs)

        id = trade_plan['_id']
        hbtrade.DB_CONN.get_collection('trade_plan').find_one_and_update({"_id": id}, {'$set': {'deal': True}})

        return buy, sell
    else:
        return None, None


def trade_operate(df, type):
    order_ids = []
    for i in range(0, len(df)):
        symbol = df.iloc[i]['symbol']
        balance = df.iloc[i]['balance']
        order_id = ''
        if type == 'sell':
            order_id = deal.sell_operate(symbol, balance)
        elif type == 'buy':
            order_id = deal.buy_operate(symbol, balance)
        if order_id is not None:
            order_ids.append(order_id)
    return order_ids


def make_trade_plan_deal():
    '''
        交易，从库里拿到交易计划，先卖再买
        市价单未成交，会一直往下吃买一价单，直到卖出，目前测试金额小无所谓，大额需要换挂单方式 todo
    '''
    buy, sell = get_trade_plan(2)

    if buy is None and sell is None:
        return

    if len(sell) > 0:
        sell_order_ids = trade_operate(sell, 'sell')
        if sell_order_ids is not None and len(sell_order_ids) > 0:
            for orderid in sell_order_ids:
                deal.solve_unfillex_order(orderid, 'sell')
            time.sleep(10)  # 卖出操作后，火币账号金额正确要过几秒钟

    if len(buy) > 0:
        buy_order_ids = trade_operate(buy, 'buy')
        if buy_order_ids is not None and len(buy_order_ids) > 0:
            for orderid in buy_order_ids:
                deal.solve_unfillex_order(orderid, 'buy')


if __name__ == '__main__':
    make_trade_plan_deal()
