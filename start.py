# -*- coding:utf-8 -*-
# author: cocola

import time
import csv
from coinhadler import coinModel
from coinhadler import dataProcess
from coinhadler import loadData



SYMBOL = ['btcusdt', 'bchusdt', 'ethusdt', 'etcusdt', 'ltcusdt', 'eosusdt', 'adausdt', 'xrpusdt', 'omgusdt', 'steemusdt', 'dashusdt',
         'iotausdt', 'zecusdt', 'hb10usdt', 'htusdt', 'ontusdt', 'iostusdt', 'zilusdt', 'btmusdt', 'socusdt', 'wiccusdt', 'thetausdt',
         'elfusdt', 'dtausdt', 'ctxcusdt', 'letusdt', 'nasusdt', 'elausdt', 'neousdt', 'trxusdt', 'qtumusdt', 'actusdt', 'venusdt',
         'hsrusdt', 'ruffusdt', 'mdsusdt', 'btsusdt', 'cmtusdt', 'itcusdt', 'ocnusdt', 'smtusdt', 'sntusdt', 'cscusdt', 'xemusdt',
         'gntusdt', 'storjusdt']


def start():
    """"""
    # 日本服务器时间为UTC时间 + 8，
    systime = (int(time.time()) + 28800)*1000

    for coin in SYMBOL:
        print("\n")
        print("<<< " + coin + " >>>, training start ... ")
        data_obj = loadData.DataLoader()
        step_df = data_obj.loadMongo(db='coin', collection='huobi_%s_step2' % coin, host='localhost')
        step_df.rename(columns={'ap': 'asks_price', 'av': 'asks_volume', 'bp': 'bids_price', 'bv': 'bids_volume'}, inplace=True)
        trade_df = data_obj.loadMongo(db='coin', collection='huobi_%s_trade' % coin, host='localhost')
        trade_df.rename(columns={'cid': 'biz_id', 'dir': 'direction', 'pid': 'parent_id', 'pr': 'price', 'vol': 'volume', 'ts_': 'trade_ts'}, inplace=True)
        # 默认feerate 0.002， 增加交易损失到0.003
        proc_obj = dataProcess.DataProcesser(trade_df, step_df, opts=systime, interval=3600, feerate=0.003)
        train_sample = proc_obj.generateSample()
        # 异常值outlier
        train_sample = proc_obj.outlier(train_sample, error=50)
        model_obj = coinModel.TrainCoinModel(train_sample)

        best_parameters = model_obj.bestModel(feature=['$BS_cnt', '$AB_cnt', '$bids_cnt', '$asks_cnt',
                                                       '$total_biz_cnt', '$total_price_cnt', '$sell_biz_cnt', '$sell_price_cnt',
                                                       '$buy_biz_cnt', '$buy_price_cnt', '$avg_bids_price', '$avg_asks_price',
                                                       '@sell_buy_rate', '@sell_buy_price','@bids_sell_rate',
                                                       '@asks_buy_rate', '@BA_rate', '@BS_price_interval', '@AB_price_interval'],
                                              label='$label')
        model = model_obj.model2JOB(savepath='/Users/dk/Downloads/' + coin,
                                feature=['$BS_cnt', '$AB_cnt', '$bids_cnt', '$asks_cnt',
                                        '$total_biz_cnt', '$total_price_cnt', '$sell_biz_cnt', '$sell_price_cnt',
                                        '$buy_biz_cnt', '$buy_price_cnt', '$avg_bids_price', '$avg_asks_price',
                                        '@sell_buy_rate', '@sell_buy_price','@bids_sell_rate',
                                        '@asks_buy_rate', '@BA_rate', '@BS_price_interval', '@AB_price_interval'],
                                label='$label',
                                best_n_estimators=best_parameters["n_estimators"],
                                best_criterion=best_parameters["criterion"],
                                best_max_depth=best_parameters["max_depth"],
                                best_max_features=best_parameters["max_features"],
                                best_min_samples_split=best_parameters["min_samples_split"])
        if model is not None:
            metric_file = open('/Users/dk/Downloads/' + coin + "/RF_metric.csv" , "a+")
            metric_writer = csv.writer(metric_file, lineterminator='\n', quotechar='\"', quoting=csv.QUOTE_NONNUMERIC)
            content = str(int(time.time()) + 28800) + "     " +  model
            metric_writer.writerow(content)

if __name__ == '__main__':
    start()




