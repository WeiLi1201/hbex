#  -*- coding: utf-8 -*-

import pandas as pd
from pymongo import MongoClient
from bson import json_util
import json
from tradetool.tool import Pandastool
from huobi.HuobiServices import HuobiApi
import time
import datetime
import math
from logger import tradesignal_logger as log


class HBTrade:
    ACCOUNT_ID = 0
    hbapi = HuobiApi()
    pdtool = Pandastool()
    mongo_uri = 'mongodb://%s:%s@%s:%s' % ('cocola_2018', 'cocola_dmx~!`', '149.28.18.43', 27017)
    DB_TRADE = MongoClient(mongo_uri, authSource='trade', authMechanism='SCRAM-SHA-1')
    DB_CONN = DB_TRADE['trade']
    # mongo_uri = 'mongodb://%s:%s@%s:%s' % ('root', 'hello123', 'localhost', 27017)
    # DB_CONN = MongoClient(mongo_uri)['test']

    def __init__(self):
        accounts = self.hbapi.get_accounts()
        acct_id = accounts['data'][0]['id']
        self.ACCOUNT_ID = acct_id

    def get_account_holding(self, acct_id=ACCOUNT_ID):
        pipeline = [
            {"$unwind": "$data.list"},
            {"$match": {"data.list.type": "trade", "data.list.balance": {'$gt': "0.0001"}}},
            {"$project": {"data.list": 1, "_id": 0}}
        ]
        holding = list(self.DB_CONN.get_collection('account').aggregate(pipeline=pipeline))
        #
        hold_list = []
        for i in range(0, len(holding)):
            hold_list.append((holding[i]['data']['list']['currency'],
                              holding[i]['data']['list']['type'],
                              holding[i]['data']['list']['balance']))
        return hold_list

    '''1分钟一次'''
    def set_currency_back_list(self):
        log.info('获取已交易币种并存库,acct_id: %s, ts: %s' % (self.ACCOUNT_ID, datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
        '''
            拿到现有非usdt头寸，将头寸加入到backlist
        '''

        hold_list = self.get_account_holding(self.ACCOUNT_ID)
        real_price_list = self.get_real_trade_price()

        hold_df = pd.DataFrame(data=hold_list, columns=['currency', 'type', 'balance'])
        real_df = pd.DataFrame(data=real_price_list, columns=['symbol', 'rt_price'])
        real_df['currency'] = real_df['symbol'].str.replace('usdt', '')
        account_currenncy_df = pd.merge(hold_df, real_df, how='left', on='currency')
        account_currenncy_df['rt_price'] = pd.to_numeric(account_currenncy_df['rt_price'], errors='coerce').fillna(
            1)
        account_currenncy_df['balance'] = pd.to_numeric(account_currenncy_df['balance'], errors='coerce').fillna(0)
        account_currenncy_df['eval_usdt'] = account_currenncy_df['rt_price'] * account_currenncy_df['balance']

        # hold_df = account_currenncy_df.query('eval_usdt > 1.0 and rt_price != 1.0 ')
        hold_df = account_currenncy_df.query('eval_usdt > 1.0')
        if len(hold_df) > 0:
            hold_list = self.get_currency_black_list()
            if hold_list is None:
                hold_set = set(hold_df['currency'])
            else:
                hold_set = set(hold_list).union(list(hold_df['currency']))

            holds = ''
            for currency in hold_set:
                holds = holds + ',' + currency

            hold_symbol = {
                'platform': 'huobi',
                'ts': datetime.datetime.utcnow(),
                'data': holds.replace(',', '', 1)
            }
            self.DB_CONN.get_collection('trade_black_list_today').replace_one({'platform': 'huobi'}, hold_symbol, True)

    def get_currency_black_list(self):
        rt_res = self.DB_CONN.get_collection('trade_black_list_today').find_one({'platform': 'huobi'}, {'data': 1, '_id': 0})
        if rt_res is None:
            return None
        else:
            base_black_list = list(rt_res['data'].split(','))
            return base_black_list

    def del_black_list(self):
        self.DB_CONN.get_collection('trade_black_list_today').delete_one({'platform': 'huobi'})

    '''异步 实时持仓，每分钟一次'''
    def set_rt_account_holding(self, acct_id=ACCOUNT_ID):
        # 拿到所有持仓，更新余额到mongo，拿出不为0的持仓返回
        log.info('获取账户实时持仓,acct_id: %s, ts: %s' % (acct_id, datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
        holding = self.hbapi.get_balance(acct_id)
        loads = json_util.loads(json.dumps(holding))
        db_res = self.DB_CONN.get_collection('account').replace_one({'data.id': acct_id}, loads, True)

    def get_account_currency(self, acct_id=ACCOUNT_ID):
        hold_list = self.get_account_holding(acct_id)
        real_price_list = self.get_real_trade_price()

        hold_df = pd.DataFrame(data=hold_list, columns=['currency', 'type', 'balance'])
        real_df = pd.DataFrame(data=real_price_list, columns=['symbol', 'rt_price'])
        real_df['currency'] = real_df['symbol'].str.replace('usdt', '')
        account_currenncy_df = pd.merge(hold_df, real_df, how='left', on='currency')
        account_currenncy_df['rt_price'] = pd.to_numeric(account_currenncy_df['rt_price'], errors='coerce').fillna(
            1)
        account_currenncy_df['balance'] = pd.to_numeric(account_currenncy_df['balance'], errors='coerce').fillna(0)
        account_currenncy_df['eval_usdt'] = account_currenncy_df['rt_price'] * account_currenncy_df['balance']

        sum_usdt = account_currenncy_df['eval_usdt'].sum()
        hold_usdt = account_currenncy_df.loc[account_currenncy_df['currency'] == 'usdt', 'eval_usdt'].values[0]

        return sum_usdt, hold_usdt

    '''异步 基准价格，昨日收盘 每10分钟一次'''

    def set_base_trade_price(self):
        tickers = self.hbapi.get_market_ticker()
        log.info('获取基准价格： ts: %s' % datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
        base_trade_list = []
        symbol_set = set()
        for ticker in tickers['data']:
            if 'usdt' in ticker['symbol']:
                symbol = ticker['symbol']
                symbol_kline = self.hbapi.get_kline(symbol, '60min', 24)
                if symbol_kline is not None and 'ok' in symbol_kline['status']:
                    for tick in symbol_kline['data']:
                        if time.localtime(tick['id']).tm_hour == 0 and time.localtime(tick['id']).tm_min == 0:
                        # if time.localtime(tick['id']).tm_hour == 16 and time.localtime(tick['id']).tm_min == 0:
                            close = tick['close']
                            if symbol not in symbol_set:
                                base_trade_list.append((symbol, close))
                                symbol_set.add(symbol)
        # base_trade_list = [('eosusdt', 8.8), ('eth', 5.2), ('trx/usdt', 0.03)]
        symbols_pd = pd.DataFrame(data=base_trade_list, columns=['symbol', 'base_price'])
        symbol__base_price = {
            'platform': 'huobi',
            'data': json_util.loads(symbols_pd.to_json(orient='records'))
        }

        self.DB_CONN.get_collection('symbol_base_price').replace_one({'platform': 'huobi'}, symbol__base_price, True)

    def get_base_trade_price(self):
        rt_res = self.DB_CONN.get_collection('symbol_base_price').find_one({'platform': 'huobi'}, {'data': 1, '_id': 0})
        base_trade_list = rt_res['data']
        return base_trade_list

    def get_real_trade_price(self):
        rt_res = self.DB_CONN.get_collection('symbol_price').find_one({'platform': 'huobi'}, {'data': 1, '_id': 0})
        # rt_price_pd = pd.DataFrame(data=rt_res['data'], columns=['symbol', 'rt_price'])
        return rt_res['data']

    '''初始化 或10分钟一次'''

    def set_symbols_precision(self):
        symbols = self.hbapi.get_symbols()  # 初始化
        log.info('获取usdt交易对价格精度： ts: %s' % datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
        symbols_pd = pd.DataFrame(data=list(symbols['data']), columns=['base-currency', 'quote-currency',
                                                                       'price-precision', 'amount-precision',
                                                                       'symbol-partition', 'symbol'])
        symbols_pd.rename(columns={'base-currency': 'base_currency', 'quote-currency': 'quote_currency',
                                   'price-precision': 'price_precision',
                                   'amount-precision': 'amount_precision', 'symbol-partition': 'symbol_partition'},
                          inplace=True)

        symbol_price = {
            'platform': 'huobi',
            'data': json_util.loads(symbols_pd.to_json(orient='records'))
        }
        self.DB_CONN.get_collection('symbol_precision').replace_one({'platform': 'huobi'}, symbol_price, True)

    def get_symbols_precision(self):
        precision_res = self.DB_CONN.get_collection('symbol_precision').find_one({'platform': 'huobi'},
                                                                                 {'data': 1, '_id': 0})
        symbols_pd = pd.DataFrame(data=precision_res['data'],
                                  columns=['base_currency', 'quote_currency', 'price_precision', 'amount_precision',
                                           'symbol_partition', 'symbol'])
        return symbols_pd

    '''异步 实时价格存库，每分钟一次'''

    def set_rt_price_to_mongo(self):
        symbols_pd = self.get_symbols_precision()

        usdt_symbols = list(symbols_pd.loc[symbols_pd.quote_currency == 'usdt'].loc[symbols_pd.symbol_partition == 'main']['symbol'])
        real_price_list = []
        log.info('获取usdt交易对 实时价格存库，ts: %s' % datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
        for symbol in usdt_symbols:
            if 'hb10' not in symbol:
                trade_detail = self.hbapi.get_trade(symbol)
                real_price = trade_detail['tick']['data'][0]['price']
                real_price_list.append((symbol, real_price))

        price_df = pd.DataFrame(data=real_price_list, columns=['symbol', 'rt_price'])

        symbol_price = {
            'platform': 'huobi',
            'ts': datetime.datetime.utcnow(),
            'data': json_util.loads(price_df.to_json(orient='records'))
        }
        self.DB_CONN.get_collection('symbol_price').replace_one({'platform': 'huobi'}, symbol_price, True)


if __name__ == '__main__':
    hbtrade = HBTrade()
    # hbtrade.get_base_trade_price()
    # hbtrade.set_symbols_precision()
    # hbtrade.set_base_trade_price()
    # hbtrade.set_rt_price_to_mongo()
    # hbtrade.get_real_trade_price()
    hbtrade.set_rt_account_holding(647052)

    # hbtrade.set_currency_back_list()
