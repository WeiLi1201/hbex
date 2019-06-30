#  -*- coding: utf-8 -*-

from huobi.HuobiServices import HuobiApi
from coinhadler import loadData
from tradetool.hb_trade import HBTrade
import datetime
import pandas as pd
from bson import json_util

hbtrade = HBTrade()

def good():
    SYMBOL = ['btcusdt', 'bchusdt', 'ethusdt', 'etcusdt', 'ltcusdt', 'eosusdt', 'adausdt', 'xrpusdt', 'omgusdt',
              'steemusdt', 'dashusdt',
              'iotausdt', 'zecusdt', 'hb10usdt', 'htusdt', 'ontusdt', 'iostusdt', 'zilusdt', 'btmusdt', 'socusdt',
              'wiccusdt', 'thetausdt',
              'elfusdt', 'dtausdt', 'ctxcusdt', 'letusdt', 'nasusdt', 'elausdt', 'neousdt', 'trxusdt', 'qtumusdt',
              'actusdt', 'venusdt',
              'hsrusdt', 'ruffusdt', 'mdsusdt', 'btsusdt', 'cmtusdt', 'itcusdt', 'ocnusdt', 'smtusdt', 'sntusdt',
              'xemusdt', 'gntusdt', 'storjusdt']
    print(len(SYMBOL))
    SYMBOL2 = ['btcusdt', 'bchusdt', 'ethusdt', 'etcusdt', 'ltcusdt', 'eosusdt', 'adausdt', 'xrpusdt', 'omgusdt',
              'steemusdt', 'dashusdt',
              'iotausdt', 'zecusdt', 'hb10usdt', 'htusdt', 'ontusdt', 'iostusdt', 'zilusdt', 'btmusdt', 'socusdt',
              'wiccusdt', 'thetausdt',
              'elfusdt', 'dtausdt', 'ctxcusdt', 'letusdt', 'nasusdt', 'elausdt', 'neousdt', 'trxusdt', 'qtumusdt',
              'actusdt', 'venusdt',
              'hsrusdt', 'ruffusdt', 'mdsusdt', 'btsusdt', 'cmtusdt', 'itcusdt', 'ocnusdt', 'smtusdt', 'sntusdt',
              'xemusdt', 'gntusdt', 'storjusdt']
    print(len(SYMBOL2))

if __name__ == '__main__':
    # sign_dic = {}
    # sign_dic['eos'] = 1
    # sign_dic['btc'] = 0
    # sign_dic['bch'] = 0
    # print(len(sign_dic))
    # good()
    hbtrade = HBTrade()
    sum_usdt,usdt = hbtrade.get_account_currency()
    pass

def another2():
    a = 2.90580000000000
    print(len(str(a).rstrip("00")) - (1 + str(a).rstrip("00").find('.')))
    # print(HuobiApi.get_symbols())
    huobiapi = HuobiApi()
    order_id = '9839906195'
    # res_orderid = huobiapi.cancel_order(order_id)
    res_orderid = huobiapi.order_info(order_id)

    data_obj = loadData.DataLoader()
    step_df = data_obj.loadMongo(db='coin', collection='huobi_btcusdt_step2', host='149.xxx.18.43', port=27017,
                                 username='xxxxxx', password='xxxxxx~!`', query={"ts": 1532944884038})
    print(step_df.head(10))

    print(res_orderid)


def another():
    string = 'eosuususdt'
    newstring = str.replace(string, 'usdt', '')

    sign_dic = {}
    sign_dic['eos'] = 1
    sign_dic['btc'] = 0
    sign_dic['bch'] = 0

    sign_pd = pd.DataFrame(data=list(sign_dic.items()), columns=['symbol', 'direction'])

    if len(sign_pd) > 2:
        sign_pd = sign_pd.head(2)

    ts = datetime.datetime.utcnow()
    data = json_util.loads(sign_pd.to_json(orient='records'))
    hbtrade.DB_CONN.get_collection('sign_detail').insert_one({
        'ts': ts,
        'data': data,
        'deal': False
    })

    print('完成')
