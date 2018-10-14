$ cat load_file_to_hive_vt.sh
#!/bin/sh

date_yes=$1
if [ ! ${date_yes} ]; then
    date_yes=`date -d "-1 days" +"%Y%m%d"`
fi
echo ${date_yes}
date_bef_yes=`date -d "${date_yes} -1 day" +"%Y%m%d"`
date_bef_7=`date -d "${date_yes} -7 day" +"%Y%m%d"`
echo ${date_bef_yes} ${date_bef_7}

remote_host="hadoop@192.168.1.27"
sour_addr="/home/data/superhero_vietnam/"
local_addr="/data/superhero_vt"
data_base="superhero_vt"
ser="\\\", \\\""
map1="\\\", \\\""
map2="\\\": \\\""

hive -S -e
use ${data_base};
load data  inpath '${local_addr}/paylog/paylog_${date_yes}' overwrite into table ${data_base}.raw_paylog partition (ds='${date_yes}');
insert overwrite table mid_paylog_all_ext partition(ds='${date_bef_yes}')
select order_id,admin,gift_coin,level,old_coin,order_coin,order_money,order_rmb,order_time,platform_2,product_id,raw_data,reason,scheme_id,
user_id from mid_paylog_all where ds='${date_bef_yes}' ;

insert overwrite table mid_paylog_all_ext partition(ds='${date_yes}')
select order_id,admin,gift_coin,level,old_coin,order_coin,order_money,order_rmb,order_time,platform_2,product_id,raw_data,reason,scheme_id,
user_id from raw_paylog where ds='${date_yes}' ;

insert overwrite table mid_paylog_all partition(ds='${date_yes}')
select order_id,admin,gift_coin,level,old_coin,order_coin,order_money,order_rmb,order_time,platform_2,product_id,raw_data,reason,scheme_id,
user_id from mid_paylog_all_ext;


insert overwrite table mid_gs_user partition(ds='${date_yes}') select distinct user_id from mid_paylog_all where ds='${date_yes}' and lower(platform_2) = 'admin_test' ;
alter table mid_paylog_all_ext drop partition(ds='${date_bef_yes}');
alter table mid_paylog_all_ext drop partition(ds='${date_yes}');
alter table mid_paylog_all drop partition(ds='${date_bef_7}');

hive -S -e
use ${data_base};
load data  inpath '${local_addr}/act/act_${date_yes}' overwrite into table ${data_base}.raw_act partition (ds='${date_yes}');

hive -S -e
use ${data_base};
load data  inpath '${local_addr}/reg/reg_${date_yes}' overwrite into table ${data_base}.raw_reg partition (ds='${date_yes}');

hive -S -e
use ${data_base};
load data  inpath '${local_addr}/info/info_${date_yes}' overwrite into table ${data_base}.raw_info partition (ds='${date_yes}');
insert overwrite table mid_info_all_ext partition (ds='${date_bef_yes}') select uid,account,nick,platform_2,device,create_time,fresh_time,vip_level,level,zhandouli,food,metal,energy,nengjing,zuanshi,qiangnengzhichen,chaonengzhichen,xingdongli,xingling,jinbi,lianjingshi,shenen,gaojishenen,gaojinengjing,jingjichangdianshu from mid_info_all where ds='${date_bef_yes}' ;
insert overwrite table mid_info_all_ext partition (ds='${date_yes}') select uid,account,nick,platform_2,device,create_time,fresh_time,vip_level,level,zhandouli,food,metal,energy,nengjing,zuanshi,qiangnengzhichen,chaonengzhichen,xingdongli,xingling,jinbi,lianjingshi,shenen,gaojishenen,gaojinengjing,jingjichangdianshu from raw_info where ds='${date_yes}' ;

insert overwrite table mid_info_all partition(ds='${date_yes}') select t.uid,t.account,t.nick,t.platform_2,t.device,t.create_time,t.fresh_time,t.vip_level,t.level,t.zhandouli,t.food,t.metal,t.energy,t.nengjing,t.zuanshi,t.qiangnengzhichen,t.chaonengzhichen,t.xingdongli,t.xingling,t.jinbi,t.lianjingshi,t.shenen,t.gaojishenen,t.gaojinengjing,t.jingjichangdianshu from (select *, row_number() over (distribute by uid sort by fresh_time desc ) as rn from mid_info_all_ext ) t where t.rn<2;

alter table mid_info_all_ext drop partition(ds='${date_bef_yes}');
alter table mid_info_all_ext drop partition(ds='${date_yes}');


hive -S -e
use ${data_base};
load data  inpath '${local_addr}/pet/pet_${date_yes}' overwrite into table ${data_base}.raw_pet partition (ds='${date_yes}');

hive -S -e
use ${data_base};
load data  inpath '${local_addr}/equip/equip_${date_yes}' overwrite into table ${data_base}.raw_equip partition (ds='${date_yes}');

hive -S -e
use ${data_base};
load data  inpath '${local_addr}/item/item_${date_yes}' overwrite into table ${data_base}.raw_item partition (ds='${date_yes}');

hive -S -e
use ${data_base};
load data  inpath '${local_addr}/card/card_${date_yes}' overwrite into table ${data_base}.raw_card partition (ds='${date_yes}');

hive -S -e
use ${data_base};
load data  inpath '${local_addr}/super_step/card_super_step_${date_yes}' overwrite into table ${data_base}.raw_super_step partition (ds='${date_yes}');

hive -S -e
use ${data_base};
load data  inpath '${local_addr}/scores/all_scores_${date_yes}' overwrite into table ${data_base}.raw_scores partition (ds='${date_yes}');

hive -S -e
use ${data_base};
load data  inpath '${local_addr}/action_log/vt_action_log_${date_yes}' overwrite into table ${data_base}.raw_action_log partition (ds='${date_yes}');

hive -S -e
use ${data_base};
load data  inpath '${local_addr}/spendlog/spendlog_${date_yes}' overwrite into table ${data_base}.raw_spendlog partition (ds='${date_yes}');
insert overwrite table mid_spendlog_no_gs partition(ds='${date_yes}') select a.order_id,a.user_id,a.level,a.subtime,a.coin_num,a.coin_1st,a.coin_2nd,a.goods_type,a.goods_subtype,a.goods_name,a.goods_num,a.goods_price,a.goods_cnname,a.args from (select * from raw_spendlog where ds='${date_yes}')a left outer join (select user_id from mid_gs_user where ds='${date_yes}')b on a.user_id=b.user_id where b.user_id is null;

hive -S -e
use ${data_base};
load data local inpath '/data/superhero/bi_tool/${data_base}/viso_config/ser_list_${date_yes}' overwrite into table ${data_base}.raw_ser_list partition(ds='${date_yes}');
insert overwrite table mid_ser_list partition(ds='${date_yes}') select explode(split(substring(ser,3,length(ser)-4),'${ser}')) from ${data_base}.raw_ser_list where ds='${date_yes}';

hive -S -e
use ${data_base};
load data local inpath '/data/superhero/bi_tool/${data_base}/viso_config/father_server_map_${date_yes}' overwrite into table ${data_base}.raw_ser_map partition(ds='${date_yes}');
insert overwrite table mid_ser_map partition(ds='${date_yes}') select explode(str_to_map(substring(ser,3,length(ser)-4),'${map1}','${map2}')) from ${data_base}.raw_ser_map where ds='${date_yes}';

