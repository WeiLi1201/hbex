#!/bin/bash

sh /root/hbex/shell/monitor_deal.sh
sleep 1s
sh /root/hbex/shell/monitor_plan.sh
sleep 1s
sh /root/hbex/shell/monitor_stoploss.sh
sleep 1s
sh /root/hbex/shell/monitor_prepare.sh
