#  -*- coding: utf-8 -*-
from coinhadler.loadData import DataLoader
from coinhadler import dataProcess
import datetime
import calendar
import time
from sklearn.externals import joblib
from tradetool.hb_trade import HBTrade
import pandas as pd
from bson import json_util
import os
from logger import cmpfeature_logger as log

hbtrade = HBTrade()

dataload = DataLoader()

SYMBOL = ['btcusdt', 'bchusdt', 'ethusdt', 'etcusdt', 'ltcusdt', 'eosusdt', 'adausdt', 'xrpusdt', 'omgusdt',
          'steemusdt', 'dashusdt',
          'iotausdt', 'zecusdt', 'hb10usdt', 'htusdt', 'ontusdt', 'iostusdt', 'zilusdt', 'btmusdt', 'socusdt',
          'wiccusdt', 'thetausdt',
          'elfusdt', 'dtausdt', 'ctxcusdt', 'letusdt', 'nasusdt', 'elausdt', 'neousdt', 'trxusdt', 'qtumusdt',
          'actusdt', 'venusdt',
          'hsrusdt', 'ruffusdt', 'mdsusdt', 'btsusdt', 'cmtusdt', 'itcusdt', 'ocnusdt', 'smtusdt', 'sntusdt',
          'xemusdt', 'gntusdt', 'storjusdt']

# SYMBOL = ['ocnusdt']

# SYMBOL = ['btcusdt', 'bchusdt', 'xrpusdt', 'etcusdt', 'socusdt']

def savesignal_mongo(signal_dict):
    sign_pd = pd.DataFrame(data=list(signal_dict.items()), columns=['symbol', 'direction'])

    ts = datetime.datetime.utcnow()
    data = json_util.loads(sign_pd.to_json(orient='records'))
    hbtrade.DB_CONN.get_collection('sign_detail').insert_one({
        'ts': ts,
        'data': data,
        'deal': False
    })
    pass


def cmp_features():
    # datetime.timedelta(hours=8)
    systime_beijing = datetime.datetime.utcnow()
    systime = int(systime_beijing.timestamp() * 1000)

    end_ts = int(systime_beijing.timestamp() * 1000)
    start_ts = int((systime_beijing - datetime.timedelta(minutes=5)).timestamp() * 1000)

    log.info('end_ts:%s' % str(end_ts))
    log.info('start_ts:%s' % str(start_ts))
    # model_dir = os.path.dirname(os.path.dirname(__file__)) + '/data'
    model_dir = os.path.dirname(os.path.dirname(__file__)) + '/model/'
    if not os.path.isdir(model_dir):
        os.makedirs(model_dir)

    # modelfile = '/Users/liwei/yuntu-inc/pyproject/hbex/data/RFcoin_model.sav'

    '''
        取数据 trade 和 step
        然后算出 feature
        得到的result 加入到signal_list
        signal_result存库
    '''
    signal_dict = {}

    for coin in SYMBOL:
        modelfile = os.path.join(model_dir, coin + '/RF_model_'+coin+'.sav')
        if not os.path.exists(modelfile):
            # os.system(r'touch %s' % modelfile
            log.error('模型文件不存在: %s' % modelfile)
            continue
        log.info("<<< " + coin + " >>>, cmp feature start ... ")
        query = {"ts": {'$lt': end_ts, '$gt': start_ts}}
        step_df = dataload.loadMongo(db='coin', collection='huobi_%s_step2' % coin, host='149.28.18.43', port=27017,
                                     username='cocola_2018', password='cocola_dmx~!`', query=query)
        step_df.rename(columns={'ap': 'asks_price', 'av': 'asks_volume', 'bp': 'bids_price', 'bv': 'bids_volume'},
                       inplace=True)
        trade_df = dataload.loadMongo(db='coin', collection='huobi_%s_trade' % coin, host='149.28.18.43', port=27017,
                                      username='cocola_2018', password='cocola_dmx~!`', query=query)
        trade_df.rename(
            columns={'cid': 'biz_id', 'dir': 'direction', 'pid': 'parent_id', 'pr': 'price', 'vol': 'volume',
                     'ts_': 'trade_ts'}, inplace=True)
        # 默认feerate 0.002， 增加交易损失到0.003
        proc_obj = dataProcess.DataProcesser(trade_df, step_df, opts=systime, interval=3600, feerate=0.003)

        feature = proc_obj.computeFeature()

        loaded_model = joblib.load(modelfile)
        result_ = loaded_model.predict(feature[['$BS_cnt', '$AB_cnt', '$bids_cnt', '$asks_cnt',
                                                '$total_biz_cnt', '$total_price_cnt', '$sell_biz_cnt',
                                                '$sell_price_cnt',
                                                '$buy_biz_cnt', '$buy_price_cnt', '$avg_bids_price', '$avg_asks_price',
                                                '@sell_buy_rate', '@sell_buy_price', '@bids_sell_rate',
                                                '@asks_buy_rate', '@BA_rate', '@BS_price_interval',
                                                '@AB_price_interval']])

        signal_dict[coin.replace('usdt', '')] = result_[0]
        log.info('交易对: %s, 预测结果: %s' % (coin, result_))

    if len(signal_dict) == 0:
        log.warn('生成交易信号为空')
    else:
        savesignal_mongo(signal_dict)


if __name__ == '__main__':
    cmp_features()
    pass
