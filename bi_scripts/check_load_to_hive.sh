#!/bin/bash

export PATH=/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin
BI_SCRIPT_DIR=/home/data/bi_scripts
DATE=$1
if [ ! $DATE ];then
    DATE=`date +%Y-%m-%d --date yesterday`
fi
DATE2=`date -d "$DATE" +%Y%m%d`
ERROR_MSG=`/usr/local/bin/python $BI_SCRIPT_DIR/scripts/check_hive_data_dev.py $DATE2 | sort`

if [ ! $ERROR_MSG ];then
    exit
else
    curl 'https://hook.bearychat.com/=bw79O/incoming/98a2c87979b8f00b9c5cc9e8190576d4' -X POST -d "payload={\"text\":\" $DATE:
    $ERROR_MSG\"}"
fi

