#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 宠物数据统计
'''
import settings
from utils import hql_to_df, update_mysql, ds_add, get_config

settings.set_env('superhero_vt')

date = '20160424'

pet_hql = '''
select vip_level,
       pet_id,
       count(is_follow) as follow_num,
       avg(refresh_num) + 0.1 as effect
from
(
    select uid, pet_id, is_follow, refresh_num, ds
    from raw_pet
    where ds >= '{date_7days_ago}' and ds <= '{date}'
) t1
left semi join
(
    select uid, max(ds) as ds
    from raw_pet
    where ds >= '{date_7days_ago}' and ds <= '{date}'
    group by uid
) t3 on (t1.uid=t3.uid and t1.ds=t3.ds)
join
(
    select uid, vip_level
    from mid_info_all
    where ds = '{date}'
) t2 on t1.uid = t2.uid
group by vip_level, pet_id
'''.format(**{
    'date': date,
    'date_7days_ago': ds_add(date, -6)
})
print pet_hql
pet_df = hql_to_df(pet_hql, 'hive')
print pet_df

item_hql = '''
select vip_level,
       item_id,
       sum(num) as item_num
from
(
    select uid, item_id, num, ds
    from raw_item
    where ds >= '{date_7days_ago}' and ds <= '{date}'
          and item_id >= 3001 and item_id <= 3009
) t1
left semi join
(
    select uid,
           max(ds) as ds
    from raw_item
    where ds >= '{date_7days_ago}' and ds <= '{date}'
          and item_id >= 3001 and item_id <= 3009
    group by uid
) t4 on (t1.uid=t4.uid and t1.ds=t4.ds)
join
(
    select uid, vip_level
    from mid_info_all
    where ds = '{date}'
) t3 on t3.uid = t1.uid
group by vip_level, item_id
'''.format(**{
    'date': date,
    'date_7days_ago': ds_add(date, -6)
})
print item_hql
item_df = hql_to_df(item_hql, 'hive')
print item_df

pet_item_map = {
    3001: 100,
    3002: 200,
    3003: 300,
    3004: 400,
    3005: 500,
    3006: 600,
    3007: 700,
    3008: 800,
    3009: 900,
}
item_df['pet_id'] = item_df.item_id.map(lambda i: pet_item_map[i])
result_df = item_df.merge(pet_df, on=['pet_id', 'vip_level'], how='outer')
result_df.to_excel('/tmp/pet_info_20160426.xlsx')
