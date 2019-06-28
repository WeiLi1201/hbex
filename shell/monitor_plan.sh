#!/bin/bash
#author: wei.li
#Monitor python script
PYTHON_HOMME=/root/anaconda3/bin/python3.6
SCRIPT_NAME=hbtrade_plan
SCRIPT_HOME=/root/hbex/hbtrade_plan.py
LOG_HOME=/root/hbex/logs/monitor.log
#check root user
if [ $(id -u) != "0" ]; then
    echo " Not the root user! Try using sudo Command ! "
    exit 1
fi

RES=$(ps -ef | grep $SCRIPT_NAME | grep -v grep | wc -l)
echo $?

if [ $RES -ne 0 ]; then
    echo $(date +%T%n%F)" $SCRIPT_NAME  status is health " >> $LOG_HOME
    exit;
fi
echo $(date +%T%n%F)" Restart $SCRIPT_NAME Services " >> $LOG_HOME

#restart Wrapper
nohup $PYTHON_HOMME $SCRIPT_HOME  >> /root/hbex/logs/temp.log  2>&1 &