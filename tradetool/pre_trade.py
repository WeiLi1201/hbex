#  -*- coding: utf-8 -*-

import pandas as pd
import datetime
import math
import time
from bson import json_util
from tradetool.hb_trade import HBTrade
from pymongo import MongoClient
from logger import tradesignal_logger as log

class PreTrade(HBTrade):
    ACCOUNT_ID = 0

    def __init__(self):
        super().__init__()

    def get_predict_sign(self, interval=30):
        log.info('尝试获取交易信号, ts: %s' % datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
        signal = self.DB_CONN.get_collection('sign_detail').find_one(
            {"ts": {'$lt': datetime.datetime.utcnow(),
                    '$gte': datetime.datetime.utcnow() - datetime.timedelta(minutes=interval)}, "deal": False},
            {"data": 1}, sort=[('ts', -1)], limit=1)

        if signal is not None:
            log.info('获取到 交易信号, ts: %s' % datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
            id = signal['_id']
            self.DB_CONN.get_collection('sign_detail').find_one_and_update({"_id": id}, {'$set': {'deal': True}})
            return signal['data']
        else:
            return None

    '''
        获取最新的数据,拿到未处理的信号
        
        
    '''
    def get_predict_sign2(self, interval=30):
        log.info('尝试获取交易信号, ts: %s' % datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
        mongo_uri = 'mongodb://%s:%s@%s:%s' % ('cocola_2018', 'cocola_dmx~!`', '149.28.18.43', 27017)
        DB_COIN = MongoClient(mongo_uri, authSource='coin', authMechanism='SCRAM-SHA-1')
        DB_COIN_CONN = DB_COIN['coin']

        # now = math.ceil((datetime.datetime.utcnow() + datetime.timedelta(hours=8)).timestamp())
        now = math.ceil((datetime.datetime.utcnow()).timestamp())
        now_before_interval = now - 120

        signal = DB_COIN_CONN.get_collection('result_currency').find_one(
            {"ts": {'$lt': now,
                    '$gte': now_before_interval}, "deal": False},
            {"data": 1, "symbol": 1}, sort=[('ts', -1)], limit=1)

        if signal is not None:
            log.info('获取到 交易信号, ts: %s' % datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
            id = signal['_id']
            DB_COIN_CONN.get_collection('result_currency').find_one_and_update({"_id": id}, {'$set': {'deal': True}})
            DB_COIN.close()
            # 转换 tuple 格式
            signal_tupe = self.signal_fmt(signal)
            return signal_tupe
        else:
            DB_COIN.close()
            return None

    def signal_fmt(self, signal):
        data = signal['data']
        symbol = signal['symbol']
        # begin_ts = math.floor(signal['data']['begin_ts']/1000)
        # end_ts = math.floor(signal['data']['end_ts']/1000)
        updown = signal['data']['updown']
        signal = {}
        signal_tuple = list()

        # 获取当前时间，如果当前时间小于end_ts，且updown是1，则返回
        # if round(datetime.datetime.now().timestamp()) < end_ts and updown == 1:
        if updown == 1:
            signal['symbol'] = str(symbol).replace('usdt','')
            signal['direction'] = updown
            # 判断当日该交易对  是否发生过交易
            log.info(str(signal))
            signal_tuple.append(signal)
            return signal_tuple
        else:
            return None



    '''
        账户持仓，持仓数量精度控制
    '''

    def get_account_trade_currency(self, acct_id=ACCOUNT_ID):
        # 拿到所有币种的价格/数量精度
        precesion_pd = self.get_symbols_precision()
        hold_list = self.get_account_holding(acct_id)
        hold_df = pd.DataFrame(data=hold_list, columns=['currency', 'type', 'balance'])
        hold_df = pd.merge(hold_df, precesion_pd.loc[precesion_pd.quote_currency == 'usdt'], how='inner',
                           left_on='currency', right_on='base_currency')
        hold_df['balance'] = hold_df['balance'].astype(float)
        for i in range(0, len(hold_df)):
            amount_precision = hold_df.iloc[i].amount_precision
            hold_df.at[(i, 'balance')] = int(hold_df.iloc[i].balance * pow(10, amount_precision)) / pow(10,
                                                                                                        amount_precision)

        rt_price_list = self.get_real_trade_price()
        rt_price_df = pd.DataFrame(data=rt_price_list, columns=['symbol', 'rt_price'])
        rt_price_df['currency'] = rt_price_df['symbol'].str.replace('usdt', '')
        rt_price_df.drop('symbol', axis=1, inplace=True)

        # 卖出总额usdt
        hold_df = pd.merge(hold_df, rt_price_df, how='inner', on='currency')
        hold_df['value'] = hold_df['balance'] * hold_df['rt_price']
        # 排除卖出额在1美元以下的品种,交易所阻止交易
        hold_df = hold_df[hold_df.value >= 1]

        return hold_df

    def is_global_env_fine(self):

        base_price_list = self.get_base_trade_price()
        real_price_list = self.get_real_trade_price()

        if len(real_price_list) != len(base_price_list):
            base_price_list = self.get_base_trade_price()
            real_price_list = self.get_real_trade_price()

        len_real = len(real_price_list)
        len_base = len(base_price_list)

        # if len_real >= len_base:
        base_pd = pd.DataFrame(data=base_price_list, columns=['symbol', 'base_price'])
        real_pd = pd.DataFrame(data=real_price_list, columns=['symbol', 'rt_price'])
        price_merge_pd = pd.merge(base_pd, real_pd, on='symbol')
        price_merge_pd['change'] = (price_merge_pd['rt_price'] - price_merge_pd['base_price']) / price_merge_pd[
            'base_price']
        # 涨跌计数
        up_change = len(price_merge_pd.loc[price_merge_pd.change >= 0.008])
        down_change = len(price_merge_pd.loc[price_merge_pd.change <= 0.008])

        if down_change / len(price_merge_pd) > 0.7:
            log.warn("大盘 usdt交易对%s对, %s涨%s跌，清仓" % (len_real, up_change, down_change))
            return True, False  # 清仓，交易
        elif up_change > down_change:
            log.info("大盘 usdt交易对%s对, %s涨%s跌，涨多跌少，正常交易" % (len_real, up_change, down_change))
            return False, True
        else:
            # todo 改为允许卖出方向的交易
            log.info("大盘 usdt交易对%s对, %s涨%s跌，跌多涨少，暂停交易" % (len_real, up_change, down_change))
            return False, False
        # else:
        #     log.warn("获取大盘信息有误，忽略本次信号，暂停交易")
        #     return False, False

    def set_tradeplan(self, keep_holds, buy_holds, sell_holds):
        ts = datetime.datetime.utcnow()
        if keep_holds is None:
            keep = []
            buy = []
        else:
            keep = json_util.loads(keep_holds.to_json(orient='records'))
            buy = json_util.loads(buy_holds.to_json(orient='records'))
        sell = json_util.loads(sell_holds.to_json(orient='records'))
        result = self.DB_CONN.get_collection('trade_plan').insert_one(
            {
                'ts': ts,
                'keep': keep,
                'buy': buy,
                'sell': sell,
                'deal': False
            }
        )

        return result.inserted_id


if __name__ == '__main__':
    hbtrade = HBTrade()
    pretrade = PreTrade()

    # pretrade.is_global_env_fine()
    # pretrade.get_predict_sign()
    # hbtrade.set_symnols_to_mongo()
    # hbtrade.get_real_trade_price()
