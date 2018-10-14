#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 越南 命运装备洗练情况
'''
import settings
from utils import hql_to_df, update_mysql, ds_add,hqls_to_dfs
from pandas import Series,DataFrame
from utils import get_config
import pandas as pd

settings.set_env('superhero_vt')

info_sql = '''
select ds,uid,vip_level from raw_info where level >= 48 and ds in ('20160601','20160602')
'''
equip_sql ='''
select ds,uid,equip_id,color,c_type,xilian
from
(select ds,uid,equip_id,substring(cast(equip_id as string),5,5) color,c_type,xilian from raw_equip
where ds in ('20160601','20160602')
 )a
left semi join
(
select max(ds) ds,uid from  raw_equip
where ds in ('20160601','20160602')
group by uid
  ) b
on a.ds = b.ds
and a.uid = b.uid
'''

info_df,equip_df = hqls_to_dfs([info_sql,equip_sql])

info_df = info_df[info_df['vip_level'] != 0]
equip_df = equip_df[(equip_df['color'] == '1')|(equip_df['color'] == '2')|(equip_df['color'] == '3')|(equip_df['color'] == '4')]
result = info_df.merge(equip_df,on=['ds','uid'])
fight_equip = result[(result['c_type'] >= 1 ) & (result['c_type'] <= 9 )]
# 上阵装备总数
fight_equip_df = fight_equip.groupby('vip_level').count().reset_index().loc[:,['vip_level','equip_id']].rename(columns={'equip_id':'equip_num'})
# 洗练过的装备数
xilian_equip = result[result['xilian'] != '{}']
xilian_equip_df = xilian_equip.groupby('vip_level').count().reset_index().loc[:,['vip_level','equip_id']].rename(columns={'equip_id':'xilian_num'})
# 洗练满级数
xilian_equip = result[result['xilian'] != '{}']
manji_equip = []
manji_vip = []
for i in range(len(xilian_equip)):
    equip_list = xilian_equip.iloc[i,3]
    vip_list = xilian_equip.iloc[i,2]
    xilian_df = xilian_equip.iloc[i,6]
    xilian_df = eval(xilian_df)
    if xilian_df['hp'] == 13440:
        manji_equip.append(equip_list)
        manji_vip.append(vip_list)
    elif xilian_df['hp'] == 10080:
        manji_equip.append(equip_list)
        manji_vip.append(vip_list)
    elif xilian_df['hp'] == 6720:
        manji_equip.append(equip_list)
        manji_vip.append(vip_list)
    else:
        continue
data = {'vip_level':manji_vip,'equip_id':manji_equip}
data_df = DataFrame(data)
data_result = data_df.groupby('vip_level').count().reset_index().loc[:,['vip_level','equip_id']].rename(columns={'equip_id':'manji_num'})


fight_equip_df.to_excel('/Users/kaiqigu/Downloads/Excel/fight_equip_df.xlsx')
xilian_equip_df.to_excel('/Users/kaiqigu/Downloads/Excel/xilian_equip_df.xlsx')
data_result.to_excel('/Users/kaiqigu/Downloads/Excel/data_result.xlsx')
