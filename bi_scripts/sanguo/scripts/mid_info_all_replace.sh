#!/bin/sh

# 三国补全量数据，当添加某些字段后，需要生成全量快照，然后修改本脚本
d='20160516'

# 拉数据
rsync -avzP admin@123.206.77.23:/data/redis_info/redis_stats/info_20160417 /home/data/sanguo_tx/redis_stats/info_20160516_all_from_server
# 上传到hive
hdfscli upload /home/data/sanguo_tx/redis_stats/info_20160516_all_from_server /tmp/tx_info_20160516_all_from_server
ssh centos1 "hive -e \"
use sanguo_tx;
load data inpath '/tmp/tx_info_20160516_all_from_server' overwrite into table mid_info_all partition (ds='20160516');
\""
snakebite -n 192.168.1.8 mv /user/hive/warehouse/sanguo_tx.db/mid_info_all/ds=20160516/tx_info_20160516_all_from_server /user/hive/warehouse/sanguo_tx.db/mid_info_all/ds=20160516/000000_0

rsync -avzP admin@120.92.2.239:/data/redis_info/redis_stats/info_20160417 /home/data/sanguo_ks/redis_stats/info_20160516_all_from_server
hdfscli upload /home/data/sanguo_ks/redis_stats/info_20160516_all_from_server /tmp/ks_info_20160516_all_from_server
ssh centos1 "hive -e \"
use sanguo_ks;
load data inpath '/tmp/ks_info_20160516_all_from_server' overwrite into table mid_info_all partition (ds='20160516');
\""
snakebite -n 192.168.1.8 mv /user/hive/warehouse/sanguo_ks.db/mid_info_all/ds=20160516/ks_info_20160516_all_from_server /user/hive/warehouse/sanguo_ks.db/mid_info_all/ds=20160516/000000_0
