#  -*- coding: utf-8 -*-

import datetime
import math

import pandas as pd
from pymongo import MongoClient

from tradetool.after_trade import AftTrade
from tradetool.make_deal import MakeDeal
from tradetool.pre_trade import PreTrade
from tradetool.tool import Pandastool
from huobi.HuobiServices import HuobiApi
from logger import tradesignal_logger as log

"""
    交易思路：
    1. 获取交易信号(1涨0跌)
        {'eos/usdt': {'price':10,direction:1},'btc/usdt':{'price':8310,direction:0},'eth/usdt':{'price':10,direction:1}}
    2. 获取账号持仓信息(1持有 0空仓)
        {'eos/usdt':1,'btc/usdt':1}
    3. 做出'第一决策'，保留eos头寸，卖出btc头寸, 买入eth头寸
    4. 参考大盘的 '第二决策'
        (1) 获取大盘信息，47个交易对，涨(涨+横盘)多跌少买入，跌多涨少不买
        (2) 获取交易价格，实时价格 >= 决策时价格 买(已有头寸则加仓1/10)
        (3) # 连续两次交易信号涨，价格也涨，忽略大盘涨跌
    5. 根据'第二决策'选出最后买入卖出交易对，先挂卖单，再挂买单
        挂单：
            挂市价单
            (1) 获取挂单数据，挂单买1价
            (2) 获取挂单数据，卖一买一差别小于0.2%,挂卖一价，大于0.2%,挂买一价
        成交：
            (1) 每10秒查询委托单状态
                卖单1分钟内未成交，价格比挂单时价格低，挂单买一价成交
                买单1分钟内未成交，价格上涨，挂单卖一价成交
    6. 中间止损
        每两分钟查询实时价格，对比持仓价格，价格相比买入时价格 下跌 2%，买一价止损
    7. 中间止盈
        价格相比买入后最高价跌5%， 买一价止盈
"""
hbapi = HuobiApi()
pdtool = Pandastool()
DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['test']

pretrade = PreTrade()
aftertrade = AftTrade()
deal = MakeDeal()


def generate_trade_plan():
    '''
        拿到账号持仓和信号，做第一决策
    '''

    acct_id = pretrade.ACCOUNT_ID

    # 信号币种
    # signs_tuple = pretrade.get_predict_sign(interval=2)
    signs_tuple = pretrade.get_predict_sign2(interval=2)
    log.info(signs_tuple)
    if signs_tuple is None:
        return
    # signs_tuple = list()
    # a = {"symbol": "neo", "direction": 1}
    # signs_tuple.append(a)

    # 拿到所有币种的价格/数量精度
    precesion_pd = pretrade.get_symbols_precision()

    signs_data = [(d['symbol'], d['direction']) for d in signs_tuple]
    sign_pd = pd.DataFrame(data=signs_data, columns=['currency', 'direction'])
    # 持仓币种
    hold_df = pretrade.get_account_trade_currency(acct_id)

    # 未冻结账户
    trade_currency = hold_df.loc[(hold_df.balance > 0) & (hold_df.type == 'trade')]

    # 要保留的头寸    ['','','']
    keep_holds = pd.merge(trade_currency, sign_pd.loc[sign_pd.direction == 1], how='inner',
                          on='currency')
    # 要清仓的头寸    ['','','']
    sell_holds = pd.merge(trade_currency, sign_pd.loc[sign_pd.direction == 0], how='inner',
                          on='currency')
    # 要买入的头寸  ['','','']
    buy_holds = pdtool.difference(sign_pd.loc[sign_pd.direction == 1], trade_currency, 'currency', 'currency')

    # 排除掉当日禁止交易的币种
    base_blacklist = pretrade.get_currency_black_list()
    if base_blacklist is not None:
        blackpd = pd.DataFrame(data=base_blacklist, columns=['currency'])
        buy_holds = pdtool.difference(buy_holds, blackpd, 'currency', 'currency')

    # 合并 价格/数量 精度信息
    buy_holds = pd.merge(buy_holds, precesion_pd.loc[precesion_pd.quote_currency == 'usdt'], how='inner',
                         left_on='currency', right_on='base_currency')

    '''
        获取大盘信息，做第二决策
    '''
    (clear, continue_ex) = pretrade.is_global_env_fine()

    if clear:
        # 大盘跌，全部清仓变成usdt
        clear_df = hold_df.query('currency != "usdt"')
        '''
        rt_price_list = pretrade.get_real_trade_price()
        rt_price_df = pd.DataFrame(data=rt_price_list, columns=['symbol', 'rt_price'])
        rt_price_df['currency'] = rt_price_df['symbol'].str.replace('usdt', '')
        rt_price_df.drop('symbol', axis=1, inplace=True)

        # 卖出总额usdt
        clear_df = pd.merge(clear_df, rt_price_df, how='inner', on='currency')
        clear_df['value'] = clear_df['balance'] * clear_df['rt_price']
        # 排除卖出额在1美元以下的品种,交易所阻止交易
        clear_df = clear_df[clear_df.value >= 1]
        '''
        log.info('大盘整体下跌，开始清仓')
        # todo 未测试
        insert_id = pretrade.set_tradeplan(None, None, clear_df)
        log.info(
            '生成清仓计划成功，计划id是: %s,时间: %s' % (insert_id, datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S %f')))

    continue_ex = True
    if continue_ex:
        '''
            算出交易资金，调整交易计划并入库 
        '''
        rt_price_list = pretrade.get_real_trade_price()
        rt_price_df = pd.DataFrame(data=rt_price_list, columns=['symbol', 'rt_price'])
        rt_price_df['currency'] = rt_price_df['symbol'].str.replace('usdt', '')
        rt_price_df.drop('symbol', axis=1, inplace=True)

        # 卖出总额usdt
        sell_holds = pd.merge(sell_holds, rt_price_df, how='inner', on='currency')
        if len(sell_holds) != 0:
            sell_holds['value'] = sell_holds['balance'] * sell_holds['rt_price']
            # 排除卖出额在1美元以下的品种,交易所阻止交易
            sell_holds = sell_holds[sell_holds.value >= 1]
            sell_value = sell_holds['value'].sum()
        else:
            sell_value = 0

        # 持仓usdt
        sum_usdt, hold_usdt = pretrade.get_account_currency()

        # 买入总额等于 待买币种数 * 账户资金*1/100    sum_usdt 1/100 (滑点 + 手续费)

        available_total = sell_value + hold_usdt - sum_usdt / 100
        consumed_total = (sum_usdt / 10) * len(buy_holds)

        # 算出买入币种的数量
        buy_holds = pd.merge(buy_holds, rt_price_df, how='inner', on='currency')
        buy_holds['buy_sum'] = sum_usdt / 10
        buy_holds['balance'] = round(buy_holds['buy_sum'] / buy_holds['rt_price'], 4)
        for i in range(0, len(buy_holds)):
            buy_holds.at[(i, 'balance')] = int(
                buy_holds.iloc[i].balance * pow(10, buy_holds.iloc[i].amount_precision)) / pow(
                10, buy_holds.iloc[i].amount_precision)

        if available_total > consumed_total:
            # 可以交易

            # tradeplan 所在的记录，暂时不用
            insert_id = pretrade.set_tradeplan(keep_holds, buy_holds, sell_holds)
            log.info(
                '生成交易计划成功，计划id是: %s,时间: %s' % (insert_id, datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S %f')))

        else:
            # 资金不足，取出保留仓位中亏损的币种，将其加入到sell_holds
            # 算出差额多少，需要卖几个持仓币种
            log.info('资金不足,重新规划仓位')
            sell_cnt = math.ceil(round((consumed_total - available_total) / sum_usdt, 2) * 10)

            if len(keep_holds) > 0:
                keep_holds['changerate'] = 1
                keep_holds['rt_price'] = 0
                keep_holds['symbol'] = 'usdt'
                for k in range(0, len(keep_holds)):
                    currency = keep_holds.iloc[k]['currency']
                    amount = keep_holds.iloc[k]['balance']
                    symbol = currency + 'usdt'
                    trade_detail = hbapi.get_trade(symbol)
                    # 实时价格
                    real_price = trade_detail['tick']['data'][0]['price']
                    keep_holds.loc[k, 'rt_price'] = real_price
                    keep_holds.loc[k, 'symbol'] = currency + 'usdt'
                    # 买入时价格
                    match_order = hbapi.orders_matchresults(symbol, types='buy-market,buy-limit,buy-ioc')
                    if len(match_order['data']) > 0:
                        match_price = match_order['data'][0]['price']
                        keep_holds.loc[k, 'changerate'] = round((real_price - float(match_price)) / float(match_price),
                                                                4)
                    else:
                        keep_holds.loc[k, 'changerate'] = 0

                pre_clear_df = keep_holds.query('changerate < 0.1').sort_values(by='changerate', ascending=True).head(
                    sell_cnt)

                keep_holds = pdtool.difference(keep_holds, pre_clear_df, 'currency', 'currency')

                sell_holds = sell_holds.append(pre_clear_df)

            insert_id = pretrade.set_tradeplan(keep_holds, buy_holds, sell_holds)
            log.info(
                '生成交易计划成功，计划id是: %s,时间: %s' % (insert_id, datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S %f')))

    else:
        '''
            两次交易信号上涨，价格也上涨，属于独立行情币种，忽略大盘，单独策略
        '''
        pass


if __name__ == '__main__':
    generate_trade_plan()
