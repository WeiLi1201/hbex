#  -*- coding: utf-8 -*-

from pymongo import MongoClient
from tradetool.after_trade import AftTrade
from tradetool.make_deal import MakeDeal
from tradetool.pre_trade import PreTrade
from huobi.HuobiServices import HuobiApi
import datetime
from logger import tradesignal_logger as log

hbapi = HuobiApi()
DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['test']
deal = MakeDeal()
afttrade = AftTrade()
pretrade = PreTrade()


def robin_stop_loss():
    '''
        1.获取账户头寸
        2.遍历持仓头寸，判断是否止盈/止损
        3.止盈/止损, 发起卖单
    '''
    acct_id = afttrade.ACCOUNT_ID

    hold_df = pretrade.get_account_trade_currency(acct_id)

    # 未冻结账户
    trade_currency = hold_df.loc[(hold_df.balance > 0) & (hold_df.type == 'trade')]

    if len(trade_currency) == 0:
        log.info('持仓为空，暂停止盈止损,ts: %s' % datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
        afttrade.clear_records()
        # afttrade 清空所有数据

    for i in range(0, len(trade_currency)):
        symbol = trade_currency.iloc[i]['currency'] + 'usdt'
        balance = trade_currency.iloc[i]['balance']

        log.info('持有币种：%s, 数量:%s ,判断止盈止损, ts: %s' % (symbol, balance, datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))

        if afttrade.take_profit(symbol):
            # 止盈
            log.warn('%s 触发 止盈 请求' % symbol)
            order_id = deal.sell_operate(symbol, balance)
            deal.solve_unfillex_order(order_id, 'sell')

        if afttrade.stop_losses(symbol):
            # 止损，卖出操作
            log.warn('%s 触发 止损 请求' % symbol)
            order_id = deal.sell_operate(symbol, balance)
            deal.solve_unfillex_order(order_id, 'sell')


if __name__ == '__main__':
    robin_stop_loss()
