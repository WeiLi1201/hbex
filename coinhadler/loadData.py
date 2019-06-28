"""
    数据加载
"""

import os
import json
import pandas as pd
import csv
import time
from websocket import create_connection
import gzip
from pymongo import MongoClient


COINS = ['btcusdt', 'bchusdt', 'ethusdt', 'etcusdt', 'ltcusdt', 'eosusdt', 'adausdt', 'xrpusdt', 'omgusdt', 'steemusdt', 'dashusdt',
         'iotausdt', 'zecusdt', 'hb10usdt', 'htusdt', 'ontusdt', 'iostusdt', 'zilusdt', 'btmusdt', 'socusdt', 'wiccusdt', 'thetausdt',
         'elfusdt', 'dtausdt', 'ctxcusdt', 'letusdt', 'nasusdt', 'elausdt', 'neousdt', 'trxusdt', 'qtumusdt', 'actusdt', 'venusdt',
         'hsrusdt', 'ruffusdt', 'mdsusdt', 'btsusdt', 'cmtusdt', 'itcusdt', 'ocnusdt', 'smtusdt', 'sntusdt', 'cscusdt', 'xemusdt',
         'gntusdt', 'storjusdt']

conn = MongoClient('localhost', 27017)

def saveMongo(strline):
    """ """
    # conn = MongoClient('localhost', 27017)
    mongodb = conn.coin
    if strline[:6] == '{"ch":':
        json_line = json.loads(strline)
        pick = json_line['ch'].split(".")[3]
        symbol = json_line['ch'].split(".")[1]
        if pick == 'detail':
            for i in range(0, len(json_line['tick']['data'])):
                ts = json_line['ts']
                parent_id = json_line['tick']['id']
                trade_ts = json_line['tick']['ts']
                amount = float(json_line['tick']['data'][i]['amount'])
                biz_id = str(json_line['tick']['data'][i]['id'])
                price = float(json_line['tick']['data'][i]['price'])
                direction = json_line['tick']['data'][i]['direction']
                trade_dict = '{"ts":' + str(ts) + ',' + \
                            '"pid":' + str(parent_id) + ',' + \
                            '"ts_":' + str(trade_ts) + ',' + \
                            '"vol":' + str(amount) + "," + \
                            '"cid":"' + str(biz_id) + '",' + \
                            '"pr":' + str(price) + ',' + \
                            '"dir":"' + direction + '"}'
                trade_dict = json.loads(trade_dict)
                collection = mongodb.get_collection("huobi_%s_trade" % symbol)
                collection.insert_one(trade_dict)
        else:
            for k in range(0, 20):
                ts = json_line["ts"]
                bids_item = json_line['tick']['bids'][k]
                asks_item = json_line['tick']['asks'][k]
                bids_price = bids_item[0]
                bids_amount = asks_item[1]
                asks_price = asks_item[0]
                asks_amount = asks_item[1]
                step_dict = '{"ts":' + str(ts) + ',' + \
                            '"bp":' + str(bids_price) + "," + \
                            '"bv":' + str(bids_amount) + "," + \
                            '"ap":' + str(asks_price) + "," + \
                            '"av":' + str(asks_amount) + '}'
                step_dict = json.loads(step_dict)
                collection_ = mongodb.get_collection("huobi_%s_step2"%symbol)
                collection_.insert_one(step_dict)


def saveCSV(strline):
    """ """
    tradepath = "/root/log/eosusdt/trade/"
    steppath = "/root/log/eosusdt/step/"
    #tradepath = "/Users/dk/PycharmProjects/sensor/log"
    #steppath = "/Users/dk/PycharmProjects/sensor/log"
    if not os.path.exists(tradepath):
        os.makedirs(tradepath)
    if not os.path.exists(steppath):
        os.makedirs(steppath)

    if strline[:6] == '{"ch":':
        json_line = json.loads(strline)
        pick = json_line['ch'].split(".")[3]
        symbol = json_line['ch'].split(".")[1]
        if pick == 'detail':
            trade_csvfile = open(tradepath + "/" + symbol + "/trade/trade_%s.csv" % time.strftime("%Y-%m-%d", time.localtime()), "a+")
            trade_writer = csv.writer(trade_csvfile, lineterminator='\n', quotechar='\"', quoting=csv.QUOTE_NONNUMERIC)
            for i in range(0, len(json_line['tick']['data'])):
                ch = json_line['ch']
                ts = json_line['ts']
                parent_id = json_line['tick']['id']
                trade_ts = json_line['tick']['ts']
                amount = float(json_line['tick']['data'][i]['amount'])
                trade_ts_ = json_line['tick']['data'][i]['ts']
                biz_id = str(json_line['tick']['data'][i]['id'])
                price = float(json_line['tick']['data'][i]['price'])
                direction = json_line['tick']['data'][i]['direction']
                trade_list = [ch, ts, parent_id, trade_ts, amount, trade_ts_, biz_id, price, direction]
                trade_writer.writerow(trade_list)
        else:
            step_csvfile = open(steppath + "/" + symbol + "/step/step_%s.csv" % time.strftime("%Y-%m-%d", time.localtime()), "a+")
            step_writer = csv.writer(step_csvfile, lineterminator='\n', quotechar='\"', quoting=csv.QUOTE_NONNUMERIC)
            #
            for k in range(0, 20):
                bids_item = json_line['tick']['bids'][k]
                asks_item = json_line['tick']['asks'][k]
                bids_price = bids_item[0]
                bids_amount = asks_item[1]
                asks_price = asks_item[0]
                asks_amount = asks_item[1]
                ts = json_line["ts"]
                ch = json_line["ch"]
                step_list = [ch, ts, bids_price, bids_amount, asks_price, asks_amount]
                step_writer.writerow(step_list)


def getHuoBi():
    """ """
    for i in range(1, 1000):
        print("connect " + str(i) + " times!")
        while (1):
            try:
                # ws = create_connection("wss://api.huobipro.com/ws")  # 国外
                ws = create_connection("wss://api.huobi.br.com/ws")  # 国内
                break
            except:
                print('connect ws error,retry...')
                time.sleep(5)

        # tradeStr="""{"sub": "market.ethusdt.kline.1min","id": "id10"}"""       # 订阅 KLine 数据
        # tradeStr = """{"sub": "market.eosusdt.trade.detail","id": "id10"}"""   # 订阅 Trade Detail 数据
        # depthStr = """{"sub": "market.eosusdt.depth.step3","id": "id10"}"""    # 订阅 depth Detail 数据

        for currency in COINS:
            tradeStr = """{"sub": "market.%s.trade.detail","id": "id10"}""" %currency
            stepStr = """{"sub": "market.%s.depth.step2","id": "id10"}""" %currency
            ws.send(tradeStr)
            ws.send(stepStr)

        while (1):
            try:
                compressData = ws.recv()
                result = gzip.decompress(compressData).decode('utf-8')
                if result[:7] == '{"ping"':
                    ts = result[8:21]
                    pong = '{"pong":' + ts + '}'
                    ws.send(pong)
                else:
                    saveMongo(result)
                    #saveCSV(result)
            except Exception as e:
                print(e)
                print("websocket closed. reconnect...")
                time.sleep(1)
                break


class DataLoader:
    """
    数据加载类
    """
    def __init__(self):
        """
        """
        self.name = 'Huobi'

    def loadCSV(self, path, columns, type):
        """
        columns: 字段名，list对象 e.g:['ch', 'ts', 'parent_id','trade_ts', 'amount', 'trade_ts_', 'biz_id', 'price', 'direction']
        type: 'File' or 'Folder'
        """
        folder_df = pd.DataFrame()
        if type == 'File':
            if os.path.isdir(path):
                raise ValueError("is not file!")
            file_df = pd.read_csv(path, names=columns)
            return file_df
        if type == 'Folder':
            if not os.path.isdir(path):
                raise ValueError("is not file folder!")
            files = os.listdir(path)
            for file in files:
                df_ = pd.read_csv(path + "/" + file, names=columns)
                folder_df = df_.append(folder_df)
            return folder_df

    # 1
    def loadMongo(self, db, collection, username=None, password=None, host='localhost', port=27017, query={}, no_id=True):
        """
        database: mongodb，数据库实例名
        collection: 表名
        query: 查询条件
        host: 主机名称
        port: 端口
        no_id:
        return: 返回df
        """
        if username and password:
            mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
            conn = MongoClient(mongo_uri)
        else:
            conn = MongoClient(host, port)

        db_ = conn[db]
        cursor = db_[collection].find(query)
        df = pd.DataFrame(list(cursor))
        # Delete the _id
        if no_id:
            del df['_id']
        return df


if __name__ == '__main__':
    getHuoBi()



