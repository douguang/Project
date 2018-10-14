#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 踢出武盟的成员信息
'''
from utils import hqls_to_dfs, ds_add, date_range
import pandas as pd
import settings_dev
from pandas import DataFrame

settings_dev.set_env('dancer_pub')

chengyuan_sql = '''
SELECT ass_id,
       guild_lv,
       player
FROM raw_association
WHERE ds = '20161226'
'''
print chengyuan_sql

info_sql = '''
SELECT
       user_id
FROM parse_info
WHERE ds>='20161220'
  AND ds <= '20161226'
group by
       user_id
'''
print info_sql

chengyuan_df,info_df = hqls_to_dfs([chengyuan_sql, info_sql])

print chengyuan_df.head(10)
print info_df.head(10)

# 所有工会的成员
chengyuan_dfs = []
for _, row in chengyuan_df.iterrows():
    for player in list(eval(row['player'])):
        chengyuan_data = DataFrame({'ass_id': row['ass_id'], 'guild_lv': row['guild_lv'],'player': [player]})
        chengyuan_dfs.append(chengyuan_data)
chengyuan_result_df = pd.concat(chengyuan_dfs)

chengyuan_result_df['active'] = chengyuan_result_df['player'].isin(info_df['user_id'])
chengyuan_result_df = chengyuan_result_df[chengyuan_result_df['active']]
print chengyuan_result_df.head(10)

result_df = chengyuan_result_df.groupby(['ass_id', 'guild_lv']).agg({
     'player': lambda g: g.count()
}).reset_index()
print result_df.head(10)

result_df.to_excel('/home/kaiqigu/Downloads/chengyuan.xlsx',index=False)





