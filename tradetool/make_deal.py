#  -*- coding: utf-8 -*-

from tradetool.hb_trade import HBTrade
import time
from logger import hbtrade_logger as log


class MakeDeal(HBTrade):
    def __init__(self):
        super().__init__()

    def sell_operate(self, symbol, balance, sell_type='market'):
        sell_symbol = symbol
        balance = balance
        # 卖出请求
        sell_order_res = {}
        bid1_price = 0

        if sell_type == 'market':
            # 市价卖
            sell_order_res = self.hbapi.send_order(str(balance), 'api', sell_symbol, 'sell-market')
        elif sell_type == 'limit':
            depth = self.hbapi.get_depth(sell_symbol, 'step0')
            bid1_price, bid1_amount = depth['tick']['bids'][0][0], depth['tick']['bids'][0][1]

            # 买一价卖, 挂单ioc（Immediate or Cancel，IOC）
            sell_order_res = self.hbapi.send_order(str(balance), 'api', sell_symbol, 'sell-limit',
                                                   str(bid1_price))

        # 判断成交逻辑
        if 'error' in sell_order_res['status']:
            log.warn(
                '%s挂卖单失败：err-code:%s, err-msg:%s' % (symbol, sell_order_res['err-code'], sell_order_res['err-msg']))
            return None
        elif 'ok' in sell_order_res['status']:
            if sell_type == 'market':
                log.info('挂卖单成功：挂单品种:%s, 挂单数量:%s, 挂单价格:%s 单号:%s' % (
                    sell_symbol, balance, '市价单', sell_order_res['data']))
            elif sell_type == 'limit':
                log.info('挂卖单成功：挂单品种:%s, 挂单数量:%s, 挂单价格:%s 单号:%s' % (
                    sell_symbol, balance, bid1_price, sell_order_res['data']))

            return sell_order_res['data']

    def buy_operate(self, symbol, balance, buy_type='market'):
        buy_symbol = symbol
        buy_amount = balance

        ask1_price = 0
        buy_order_res = {}

        depth = self.hbapi.get_depth(buy_symbol, 'step0')
        ask1_price, ask1_amount = depth['tick']['asks'][0][0], depth['tick']['asks'][0][1]
        if buy_type == 'market':
            # 市价  todo 已取消市价买入，都变成买一卖一成交，异常订单要做 异常成交判断
            buy_order_res = self.hbapi.send_order(str(buy_amount), 'api', buy_symbol, 'buy-limit', ask1_price)
        elif buy_type == 'limit':
            # 卖一价买
            buy_order_res = self.hbapi.send_order(str(buy_amount), 'api', buy_symbol, 'buy-limit', ask1_price)

        # 判断成交逻辑
        if 'error' in buy_order_res['status']:
            log.warn('%s挂买单失败：err-code:%s, err-msg:%s' % (symbol, buy_order_res['err-code'], buy_order_res['err-msg']))
            if 'amount-min' in buy_order_res['err-code']:
                order_id = self.buy_operate(symbol, balance, buy_type='limit')
                return order_id
            elif 'balance-insufficient' in buy_order_res['err-code']:
                time.sleep(5)
                order_id = self.buy_operate(symbol, balance, buy_type='limit')
                return order_id
            elif 'invalid-amount' in buy_order_res['err-code']:
                # 成交量 > 委托量，火币强卖
                log.error('成交量 > 委托量 异常订单：symbol:%s,balance:%s,buy_type:%s' % (symbol, balance, buy_type))
                return None
        elif 'ok' in buy_order_res['status']:
            if buy_type == 'market':
                # log.info('挂买单成功：挂单品种:%s, 挂单数量:%s, 挂单价格:%s 单号:%s' % (buy_symbol, buy_amount, '市价单', buy_order_res['data']))
                log.info('挂买单成功：挂单品种:%s, 挂单数量:%s, 挂单价格:%s 单号:%s' % (
                buy_symbol, buy_amount, ask1_price, buy_order_res['data']))
            elif buy_type == 'limit':
                log.info('挂买单成功：挂单品种:%s, 挂单数量:%s, 挂单价格:%s 单号:%s' % (
                    buy_symbol, buy_amount, ask1_price, buy_order_res['data']))
            return buy_order_res['data']

    def cancel_order(self, order_id):
        res_orderid = self.hbapi.cancel_order(order_id)
        if 'ok' == res_orderid['status']:
            order_info = self.hbapi.order_info(order_id=res_orderid['data'])
            if order_info['data']['state'] in ['canceled', 'partial-canceled']:
                log.info('取消订单成功,订单编号: %s' % order_id)
                return True
            else:
                log.info('取消订单成功,订单编号: %s,订单状态: %s' % (order_id, order_info['data']['state']))
                return False
        elif 'error' == res_orderid['status']:
            log.warn('取消订单失败,err-code: %s , err-msg: %s , data: %s' % (
                res_orderid['err-code'], res_orderid['err-msg'], res_orderid['data']))
            if 'orderstate-error' in res_orderid['err-code']:
                res_orderinfo = self.hbapi.order_info(order_id)
                if res_orderinfo['data']['state'] == 'filled' and res_orderinfo['data']['field-amount'] == \
                        res_orderinfo['data']['amount']:
                    log.info('订单已成交，无法取消订单，订单号: %s' % order_id)
            return False

    def solve_unfillex_order(self, orderid, type='sell'):
        # 卖单 部分成交或未成交的，撤销，获取买一价和市场价，选择价高的挂卖单
        order_info = self.hbapi.order_info(order_id=orderid)
        if 'ok' in order_info['status']:
            order_state = order_info['data']['state']

            if order_state == 'filled' \
                    and order_info['data']['field-amount'] == order_info['data']['amount']:
                # 卖单才存在 成交数量 等于 挂单数量
                log.info('%s 委托单完全成交,委托单号: %s' % (type, orderid))
            elif order_state in ['submitted', 'partial-filled', 'filled']:
                amount = order_info['data']['amount']
                field_amount = order_info['data']['field-amount']
                unfield = float(amount) - float(field_amount)  # 未成交的数量
                symbol = order_info['data']['symbol']
                # 挂单数量
                decimal_cnt = len(str(amount).rstrip("00")) - (1 + str(amount).rstrip("00").find('.'))
                unfield = int(unfield * pow(10, decimal_cnt)) / pow(10, decimal_cnt)
                # 先撤单再挂单卖
                re_orderid = 0

                # 已成交不需要挂撤单，直接重新挂单
                if order_state == 'filled':
                    canceled_order = True
                else:
                    time.sleep(2)
                    canceled_order = self.cancel_order(orderid)

                if canceled_order:
                    if type == 'sell':
                        re_orderid = self.sell_operate(symbol, unfield, sell_type='limit')
                    elif type == 'buy':
                        re_orderid = self.buy_operate(symbol, unfield, buy_type='limit')

                    if re_orderid is not None:
                        self.solve_unfillex_order(re_orderid, type)
        else:
            log.error('获取 %s 委托单信息失败,委托单号: %s' % (type, orderid))


if __name__ == '__main__':
    deal = MakeDeal()
    # deal.solve_unfillex_order()
