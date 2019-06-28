#  -*- coding: utf-8 -*-
from pymongo import MongoClient
import datetime

mongo_uri = 'mongodb://%s:%s@%s:%s' % ('cocola_2018', 'cocola_dmx~!`', '149.28.18.43', 27017)
DB_TRADE = MongoClient(mongo_uri, authSource='coin', authMechanism='SCRAM-SHA-1')
DB_CONN = DB_TRADE['coin']


COINS = ['btcusdt', 'bchusdt', 'ethusdt', 'etcusdt', 'ltcusdt', 'eosusdt', 'adausdt', 'xrpusdt', 'omgusdt', 'steemusdt', 'dashusdt',
         'iotausdt', 'zecusdt', 'hb10usdt', 'htusdt', 'ontusdt', 'iostusdt', 'zilusdt', 'btmusdt', 'socusdt', 'wiccusdt', 'thetausdt',
         'elfusdt', 'dtausdt', 'ctxcusdt', 'letusdt', 'nasusdt', 'elausdt', 'neousdt', 'trxusdt', 'qtumusdt', 'actusdt', 'venusdt',
         'hsrusdt', 'ruffusdt', 'mdsusdt', 'btsusdt', 'cmtusdt', 'itcusdt', 'ocnusdt', 'smtusdt', 'sntusdt', 'cscusdt', 'xemusdt',
         'gntusdt', 'storjusdt']

if __name__ == '__main__':
    systime_beijing = datetime.datetime.utcnow()

    start_ts = int((systime_beijing - datetime.timedelta(days=10)).timestamp() * 1000)

    query = {"ts": {'$lt': start_ts}}

    for currency in COINS:
        collection_step = 'huobi_%s_step2' % currency
        collection_trade = 'huobi_%s_trade' % currency
        collection_kline = 'huobi_%s_kline' % currency

        res = DB_CONN.get_collection(collection_step).delete_many(query)
        if res.acknowledged:
            print('del %s less than %s delete_count= %s' % (collection_step, start_ts, res.deleted_count))

        res = DB_CONN.get_collection(collection_trade).delete_many(query)
        if res.acknowledged:
            print('del %s less than %s delete_count= %s' % (collection_trade, start_ts, res.deleted_count))

        res = DB_CONN.get_collection(collection_kline).delete_many(query)
        if res.acknowledged:
            print('del %s less than %s delete_count= %s' % (collection_kline, start_ts, res.deleted_count))

