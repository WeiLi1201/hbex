#  -*- coding: utf-8 -*-

import pandas as pd
from pymongo import ReturnDocument

from tradetool.hb_trade import HBTrade
from huobi.HuobiServices import HuobiApi
from logger import tradesignal_logger as log

class AftTrade(HBTrade):
    '''
        止损，相比买入价跌2%止损
    '''

    def __init__(self):
        super().__init__()

    def stop_losses(self, symbol):
        # 买入时价格
        match_order = self.hbapi.orders_matchresults(symbol, types='buy-market,buy-limit,buy-ioc')
        if len(match_order['data']) == 0:
            log.info("近两月 %s 没有持仓，不需要止损，若有持仓，请手动" % symbol)
            return False

        match_price = float(match_order['data'][0]['price'])

        trade_detail = self.hbapi.get_trade(symbol)
        # 实时价格
        real_price = trade_detail['tick']['data'][0]['price']

        change = round((real_price - match_price) / match_price, 4)
        if change <= -0.02:
            log.warn("%s 买入价%s 现价%s，达到止损条件 " % (symbol, match_price, real_price))
            self.DB_CONN.get_collection('takeprofit').delete_one({'symbol': symbol})
            return True

        return False

    '''
        止盈, 相比买入后最高价回撤5%,止盈
    '''

    def take_profit(self, symbol):
        # 买入时价格
        match_order = self.hbapi.orders_matchresults(symbol, types="buy-limit,buy-market,buy-ioc")
        if len(match_order['data']) == 0:
            log.info("近两月 %s 没有持仓，不需要止盈, 若有持仓，请手动" % symbol)
            return False

        match_price = match_order['data'][0]['price']
        match_time = match_order['data'][0]['created-at']

        # match_price = 0.0133
        # match_time = 1494870000

        # 实时价格
        trade_detail = self.hbapi.get_trade(symbol)
        real_price = trade_detail['tick']['data'][0]['price']
        real_time = trade_detail['tick']['data'][0]['ts']

        rt_data = {'rt_price': real_price, 'rt_time': int(real_time), 'created_at': int(match_time),
                   'price': match_price, 'is_hold': True}

        # 取出最大价格
        max_price = self.DB_CONN.get_collection('takeprofit').find_one(
            {'symbol': symbol}, {'max_price': 1, '_id': 0}
        )

        if max_price is not None:
            if len(max_price) != 0 and real_price > float(max_price['max_price']):
                rt_data = {'rt_price': real_price, 'rt_time': int(real_time), 'created_at': int(match_time),
                           'price': match_price,
                           'max_price': round(float(real_price), 4), 'max_price_time': int(real_time)}
            elif len(max_price) == 0:
                rt_data = {'rt_price': real_price, 'rt_time': int(real_time), 'created_at': int(match_time),
                           'price': match_price, 'max_price': round(float(match_price), 4), 'max_price_time': int(match_time)}

        update_res = self.DB_CONN.get_collection('takeprofit').find_one_and_update(
            {
                'symbol': symbol
            },
            {'$set': rt_data},
            upsert=True,
            return_document=ReturnDocument.AFTER
        )

        # 最大值有值 且 最大值与现价 回撤是否大于5%
        if max_price is not None and len(max_price) != 0:
            max_price = update_res['max_price']
            rt_price = update_res['rt_price']
            rise_back_point = float(max_price) * 0.95
            if rt_price < rise_back_point:
                log.warn("%s 买入价%s 最高价%s 现价%s，达到止盈条件 " % (symbol, match_price, max_price, real_price))
                self.DB_CONN.get_collection('takeprofit').delete_one({'symbol': symbol})
                return True

        return False

    def clear_records(self):
        res = self.DB_CONN.get_collection('takeprofit').delete_many({'is_hold': True})
        log.info('清空 各交易对-最高价状态记录')
