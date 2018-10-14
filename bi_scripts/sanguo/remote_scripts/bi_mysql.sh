#!/bin/sh

echo "开始执行：$0"
local_ip=`/sbin/ifconfig | grep "inet addr:" | awk -F: '{print $2}' | awk '{print $1}' | head -1`

platform=$1
date_y=$2
if [ ! ${date_y} ];then
    date_y=`date +%Y%m%d --date '1 days ago'`
fi

database='genesis'
paylog_table='paylog'
spendlog_table='spendlog'
if [[ $platform == 'pub_js' ]];then
    mysql_ip='10.253.9.153'
    password='3ae0ae60021d5482'
elif [[ $platform == 'pub_ios' ]];then
    mysql_ip='10.253.9.153'
    password='3ae0ae60021d5482'
    paylog_table='paylog_ios'
    spendlog_table='spendlog_ios'
    database='genesis_ios'
elif [[ $platform == 'pub_tencent' ]]; then
    mysql_ip='10.141.39.9'
    password='3ae0ae60021d5482'
elif [[ $platform == 'pub_tw_gpn' ]]; then
    mysql_ip='192.168.10.66'
    # mysql_ip='10.10.1.56'
    password='0eec04c823a1'
else
    echo 'platform 参数错误'
    exit 1
fi


spendlog="/data/bi_data/${platform}/spendlog/spendlog_${date_y}"
pylog="/data/bi_data/${platform}/paylog/paylog_${date_y}"
rm -rf ${spendlog} ${pylog}

for i in {0..15}
do
        mysql -h${mysql_ip} -uroot -p${password} ${database} -Ne "select * from ${paylog_table}_$i where replace(substr(order_time,1,10),'-','')=${date_y};" >> ${pylog}
done

for i in {0..15}
do
        mysql -h${mysql_ip} -uroot -p${password} ${database} -Ne "select * from ${spendlog_table}_$i where replace(substr(subtime,1,10),'-','')=${date_y};" >> ${spendlog}
done
