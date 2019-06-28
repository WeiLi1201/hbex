#!/bin/bash

/usr/local/python3/bin/python3 /root/hbex/hbtrade_prepare.py  > /dev/null 2>&1 &
sleep 100s
/usr/local/python3/bin/python3 /root/hbex/hbtrade_cmpfeature.py  > /dev/null 2>&1 &
sleep 10s
/usr/local/python3/bin/python3 /root/hbex/hbtrade_plan.py  > /dev/null 2>&1 &
sleep 10s
/usr/local/python3/bin/python3 /root/hbex/hbtrade_deal.py  > /dev/null 2>&1 &
sleep 10s
/usr/local/python3/bin/python3 /root/hbex/hbtrade_robin_stop_loss.py  > /dev/null 2>&1 &

# 定时删除数据
sleep 20s
/usr/local/python3/bin/python3 /root/hbex/data/del_historydata.py  > /dev/null 2>&1 &