"""
    特征处理 + 样本
"""

import pandas as pd
import numpy as np


def dropDup(df, ts, dupcolumns):
    """
    df: 输入数据
    ts: 时间戳
    dupcolumns: 去重的字段list
    return: 去重后的数据
    """
    df_ = df.copy()
    df_["$ts_"] = df_[ts].map(lambda x: int(x/1000))
    dupcolumns_ = dupcolumns + ['$ts_']
    df_ = df_.groupby(dupcolumns_).first().reset_index()
    return df_

# 2
def cmptTag(df, opts, interval, bias=0, input_col='trade_ts'):
    """
    df: 输入的数据表
    opts: 当前job运行时间戳 (ms)
    interval: 处理间隔（900s／1800s／3600s）(s)
    bias: 偏移量（默认为0, 步长60s）
    input_col: 交易记录的时间戳 (ms)
    output_col:
    return: 分组标签 0-672;
    """
    def tradeTag(ts, opts, interval, bias):
        tag = int(((opts / 1000 - bias) - ts / 1000) / interval)
        # 样本唯一建标识：e.g 1530115200|1
        return str(int(opts / 1000) - bias) + "|" + str(tag)
    df_ = df.copy()
    df_['$index'] = df_[input_col].map(lambda x: tradeTag(x, opts=opts, interval=interval, bias=bias))
    return df_


def cmptLabel(df, fee=0.002, input_col='price'):
    """
    df: 根据tag分组统计，首笔订单价格，末笔订单价格，均价，最高价，最低价
    index: 唯一键标识
    in_col: 输入字段
    out_col: 输出字段
    return: dateframe
    """
    df_last = df.groupby('$index').last()[input_col].reset_index()
    df_last.columns = ['$index', '$rate']

    # 计算收益
    def rate(x):
        """ """
        x[["opts", "interval_id"]] = x['$index'].str.split("|", expand=True).astype(int)
        # 排序
        x = x.sort_values(by=["opts", "interval_id"], ascending=[True, False])
        df_rate = pd.DataFrame(columns=['$index', '$rate'])
        for i in range(0, len(x)-1):
            if (x.iloc[i+1]["interval_id"] - x.iloc[i]["interval_id"]) == -1 and x.iloc[i]["interval_id"] > 0:
                df_rate.loc[i] = [x.iloc[i]['$index'], (x.iloc[i + 1]['$rate'] - x.iloc[i]['$rate'])/x.iloc[i]['$rate']]
            else:
                df_rate.loc[i] = [x.iloc[i]['$index'], None]
        return df_rate
    # 合并
    #df_merge = pd.merge(pd.merge(pd.merge(df_first, df_last, on='index'), df_,on='index'), rate(df_last), on='index')
    # from functools import reduce
    # dfs = [df_first, df_last, rate(df_last)]
    # df_merge = reduce(lambda left, right: pd.merge(left, right, on='index'), dfs)
    df_ = rate(df_last)
    df_ = df_.loc[pd.isnull(df_["$rate"]) == False]
    # 考虑火币手续费0.002
    df_['$label'] = df_["$rate"].apply(lambda x: 1 if x > 1 / pow((1 - fee), 2) - 1 else 0)
    return df_

# 3
def cmptFeature(trade_df, step_df):
    """ """
    # 特征抽取
    trade_ = trade_df.groupby(["$index"]).last()["price"].reset_index()
    trade_.columns = ['$index', '$last_price']
    # sell_
    sell_ = trade_df[trade_df["direction"] == 'sell'].groupby("$index").agg({"price": np.mean, "volume": np.sum})[["price", "volume"]].reset_index()
    sell_.columns = ["$index", "$avg_sell_price", "$avg_sell_volume"]
    # buy_
    buy_ = trade_df[trade_df["direction"] == 'buy'].groupby("$index").agg({"price": np.mean, "volume": np.sum})[["price", "volume"]].reset_index()
    buy_.columns = ["$index", "$avg_buy_price", "$avg_buy_volume"]
    # bids_
    bids_ = step_df[step_df["bids_price"] > 0].groupby("$index").agg({"bids_price": np.mean})["bids_price"].reset_index()
    bids_.columns = ["$index", "$avg_bids_price"]
    # asks_
    asks_ = step_df[step_df["asks_price"] > 0].groupby("$index").agg({"asks_price": np.mean})['asks_price'].reset_index()
    asks_.columns = ["$index", "$avg_asks_price"]

    from functools import reduce
    dfs = [trade_, sell_, buy_, bids_, asks_]
    S = reduce(lambda left, right: pd.merge(left, right, on='$index'), dfs)
    S[["#opts", "#intId"]] = S['$index'].str.split("|", expand=True).astype(int)
    S = S.sort_values(by=["#opts", "#intId"], ascending=[True, False])

    S_ = pd.DataFrame(columns=['$index', '#opts', '#intId', '$last_price', "$last_price_nt",
                               '$avg_bids_price_0', '$avg_asks_price_0', '$avg_sell_price_0', '$avg_buy_price_0', "$avg_sell_volume_0", "$avg_buy_volume_0",
                               '$avg_bids_price_1', '$avg_asks_price_1', '$avg_sell_price_1', '$avg_buy_price_1', "$avg_sell_volume_1", "$avg_buy_volume_1",
                               '$avg_bids_price_2', '$avg_asks_price_2', '$avg_sell_price_2', '$avg_buy_price_2', "$avg_sell_volume_2", "$avg_buy_volume_2",
                               '$avg_bids_price_3', '$avg_asks_price_3', '$avg_sell_price_3', '$avg_buy_price_3', "$avg_sell_volume_3", "$avg_buy_volume_3",
                               '$avg_bids_price_4', '$avg_asks_price_4', '$avg_sell_price_4', '$avg_buy_price_4', "$avg_sell_volume_4", "$avg_buy_volume_4"])
    for i in range(0, len(S) - 1):
        if (S.iloc[i]["#intId"] - S.iloc[i - 1]["#intId"]) == -1 \
                and (S.iloc[i - 1]["#intId"] - S.iloc[i - 2]["#intId"]) == -1 \
                and (S.iloc[i - 2]["#intId"] - S.iloc[i - 3]["#intId"]) == -1 \
                and (S.iloc[i - 3]["#intId"] - S.iloc[i - 4]["#intId"]) == -1 \
                and (S.iloc[i - 4]["#intId"] - S.iloc[i - 5]["#intId"]) == -1 \
                and S.iloc[i]["#intId"] > 0 and i >= 5:
            S_.loc[i] = [S.iloc[i]['$index'], S.iloc[i]['#opts'], S.iloc[i]['#intId'], S.iloc[i]['$last_price'], S.iloc[i + 1]['$last_price'],
                         S.iloc[i]['$avg_bids_price'], S.iloc[i]['$avg_asks_price'], S.iloc[i]['$avg_sell_price'],
                         S.iloc[i]['$avg_buy_price'], S.iloc[i]['$avg_sell_volume'], S.iloc[i]['$avg_buy_volume'],
                         S.iloc[i - 1]['$avg_bids_price'], S.iloc[i - 1]['$avg_asks_price'], S.iloc[i - 1]['$avg_sell_price'],
                         S.iloc[i - 1]['$avg_buy_price'], S.iloc[i - 1]['$avg_sell_volume'], S.iloc[i - 1]['$avg_buy_volume'],
                         S.iloc[i - 2]['$avg_bids_price'], S.iloc[i - 2]['$avg_asks_price'], S.iloc[i - 2]['$avg_sell_price'],
                         S.iloc[i - 2]['$avg_buy_price'], S.iloc[i - 2]['$avg_sell_volume'], S.iloc[i - 2]['$avg_buy_volume'],
                         S.iloc[i - 3]['$avg_bids_price'], S.iloc[i - 3]['$avg_asks_price'], S.iloc[i - 3]['$avg_sell_price'],
                         S.iloc[i - 3]['$avg_buy_price'], S.iloc[i - 3]['$avg_sell_volume'], S.iloc[i - 3]['$avg_buy_volume'],
                         S.iloc[i - 4]['$avg_bids_price'], S.iloc[i - 4]['$avg_asks_price'], S.iloc[i - 4]['$avg_sell_price'],
                         S.iloc[i - 4]['$avg_buy_price'], S.iloc[i - 4]['$avg_sell_volume'], S.iloc[i - 4]['$avg_buy_volume']]
        else:
            S_.loc[i] = [S.iloc[i]['$index'], None, None, None, None,
                         None, None, None, None, None, None, None, None, None, None,
                         None, None, None, None, None, None, None, None, None, None,
                         None, None, None, None, None, None, None, None, None, None]
    return S_


def combineXY(df_X, df_Y, tag='$index'):
    """
    df_X: 特征变量
    df_Y: 目标变量
    return: df
    """
    return pd.merge(df_X, df_Y, on=tag)


class DataProcesser:
    """"""
    def __init__(self, trade_df, step_df, opts, interval, feerate):
        """
        raw_df: 输入的数据框
        raw_columns: 原始字段名
        opts: job运行时间
        interval: 时间间隔
        feerate: 费率
        """
        self.trade_df = trade_df
        self.step_df = step_df
        self.opts = opts
        self.interval = interval
        self.feerate = feerate

    def generateSample(self):
        """
        raw_df: dataframe对象，解析完的交易记录文件
        opts: 当前job运行时间戳 (ms)
        interval: 处理间隔（900s／1800s／3600s）(s)
        bias: 偏移量（默认为0, 步长60s）
        return: 分组标签 0-672;
        """
        # 去重
        #print("start... " + "\n")
        print("trade data size: " + str(len(self.trade_df)) + ", step data size: " + str(len(self.step_df)) + "\n")
        drop_trade = dropDup(self.trade_df, ts='trade_ts', dupcolumns=['parent_id','volume', 'biz_id', 'price', 'direction'])
        drop_step = dropDup(self.step_df, ts='ts', dupcolumns=['bids_price', 'bids_volume', 'asks_price', 'asks_volume'])
        # 选择基础数据列
        trade_data = drop_trade[["parent_id", "biz_id", "volume", "price", "trade_ts", "direction"]]
        step_data = drop_step[['ts', 'bids_price', 'bids_volume', 'asks_price', 'asks_volume']]
        print("dropdup trade data size: " + str(len(trade_data)) + ", dropdup step data size: " + str(len(step_data)) + "\n")

        if self.interval == 900:        # 15分钟
            list_step = list(range(0, 900, 60))
        elif self.interval == 1800:     # 30分钟
            list_step = list(range(0, 1800, 60))
        elif self.interval == 3600:     # 60分钟
            list_step = list(range(0, 3600, 60))
        else:
            raise ValueError('Wrong parameter! interval limit:900/1800/3600.')
        train_sample = pd.DataFrame()
        for i in list_step:
            train_tag = cmptTag(trade_data, self.opts, self.interval, bias=i, input_col='trade_ts')
            step_tag = cmptTag(step_data, self.opts, self.interval, bias=i, input_col='ts')
            train_label = cmptLabel(train_tag, fee=self.feerate)                # 计算标签列
            train_feature = cmptFeature(train_tag, step_tag)                    # 计算特征
            train_XY = combineXY(train_feature, train_label)
            print("=========> sample build iteration interval (", i, "s) successful!")
            train_sample = train_sample.append(train_XY)

        print("\n")
        print("===================> labe (0:1) <===================== " + "\n")
        print(train_sample.groupby("$label").agg({"$index": np.size}))
        print("\n")

        return train_sample


    def outlier(self, sample_df, error=30):
        """
        sample_df:
        return:
        """
        sample_df['$outlier'] = abs(sample_df['$sell_price_cnt'] - sample_df['$BS_cnt'])
        train_sample = sample_df[sample_df['$outlier'] <= error]
        return train_sample

    def computeFeature(self):
        """
                raw_df: dataframe对象，解析完的交易记录文件
                opts: 当前job运行时间戳 (ms)
                interval: 处理间隔（900s／1800s／3600s）(s)
                bias: 偏移量（默认为0, 步长60s）
                return: 分组标签 0-672;
                """
        # 去重
        # print("start... " + "\n")
        print("trade data size: " + str(len(self.trade_df)) + ", step data size: " + str(len(self.step_df)) + "\n")
        drop_trade = dropDup(self.trade_df, ts='trade_ts',
                             dupcolumns=['parent_id', 'volume', 'biz_id', 'price', 'direction'])
        drop_step = dropDup(self.step_df, ts='ts',
                            dupcolumns=['bids_price', 'bids_volume', 'asks_price', 'asks_volume'])

        interval = 60

        train_tag = cmptTag(drop_trade, self.opts, interval, bias=0, input_col='trade_ts')
        step_tag = cmptTag(drop_step, self.opts, interval, bias=0, input_col='ts')
        train_feature = cmptFeature(train_tag, step_tag)  # 计算特征

        return train_feature
