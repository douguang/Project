#!/bin/sh

echo "开始执行：$0"
echo "[`date`] $0 start to run!" >> /data/bi_script_new/log.txt
platform=$1
if [[ $platform == 'pub_js' ]];then
    echo '金山平台'
elif [[ $platform == 'pub_ios' ]]; then
    echo '金山ios平台'
elif [[ $platform == 'pub_tencent' ]]; then
    echo '腾讯平台'
elif [[ $platform == 'pub_tw_gpn' ]]; then
    echo '台湾平台'
else
    echo 'platform 参数错误'
    exit 1
fi

date_today=$2
if [ ! ${date_today} ];then
    date_today=`date -d yesterday +%Y%m%d`
fi

bi_snapshot() {
    if [[ $platform == 'pub_tw_gpn' ]]; then
        # 台湾的快照由 nginx 机器做了主动传过来
        echo 'pub_tw_gpn, skip snapshot'
    else
        # redis_info
        /usr/local/bin/python2.7 /data/sites/backend/scrips/bi_script/standard_get_redis_info.py ${platform} ${date_today} && \
        # all_association_info
        /usr/local/bin/python2.7 /data/sites/backend/scrips/bi_script/all_association_info.py ${platform} ${date_today} && \
        # /usr/local/bin/python2.7 /data/bi_script_new/all_association_info.py ${platform} ${date_today} && \
        # daily_honor_rank
        /usr/local/bin/python2.7 /data/sites/backend/scrips/bi_script/daily_honor_rank.py ${platform} ${date_today} && \
        # hour_city_num
        /usr/local/bin/python2.7 /data/sites/backend/scrips/bi_script/hour_city_num.py ${platform} ${date_today}
    fi
}

bi_snapshot && \
# mysql消费记录
/bin/sh /data/bi_script_new/bi_mysql.sh ${platform} ${date_today} && \
# python日志
/bin/sh /data/bi_script_new/python_log.sh ${platform} ${date_today} && \
echo 'done' || \
curl 'https://hook.bearychat.com/=bw79O/incoming/0f79b0211fcffac6e577da80fbbe928a' -X POST -d "payload={\"text\":\"【`date +%Y-%m-%d:%H:%M:%S`】$0 @ $1 数据生成失败\"}"
echo "[`date`] $0 ends!" >> /data/bi_script_new/log.txt
