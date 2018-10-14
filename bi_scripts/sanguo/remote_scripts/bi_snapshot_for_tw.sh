#!/bin/sh

echo "开始执行：$0"
date_today=$2
if [ ! ${date_today} ];then
    date_today=`date -d yesterday +%Y%m%d`
fi

/usr/local/bin/python2.7 /data/sites/backend/scrips/bi_script/standard_get_redis_info.py pub_tw ${date_today} && \
/usr/local/bin/python2.7 /data/sites/backend/scrips/bi_script/all_association_info.py pub_tw ${date_today} && \
/usr/local/bin/python2.7 /data/sites/backend/scrips/bi_script/daily_honor_rank.py pub_tw ${date_today} && \
/usr/local/bin/python2.7 /data/sites/backend/scrips/bi_script/hour_city_num.py pub_tw ${date_today} && \
rsync -avz --partial-dir=.rsync-partial /data/bi_data/pub_tw/* admin@192.168.10.86:/data/bi_data/pub_tw_gpn && \
echo "$0 done!"
