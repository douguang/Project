#!/usr/bin/env bash

for pt in sanguo_ks_nginx sanguo_tx_nginx; do
    echo ${pt} && \
    rsync -P /Users/huwenchao/kaiqigu/bi/sanguo/dis_2nd/sanguo/common/nginx_alalysis.py admin@${pt}:/home/admin/huwenchao
    # ssh admin@${pt} 'ps aux | grep nginx_mac_device.py | grep -v grep | awk '{ print $2 }' | xargs -I {} kill -9 {}; cd /home/admin/huwenchao && nohup python nginx_mac_device.py &'
done;

