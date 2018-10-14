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

settings_dev.set_env('dancer_tw')

chengyuan_sql = '''
SELECT ds,
       ass_id,
       name,
       player
FROM raw_association
WHERE ds>='20161010'
  AND ds <= '20161014'
  AND ass_id in('tw1596111','tw2071411','tw1970663')
ORDER BY ds
'''
info_sql = '''
SELECT ds,
       user_id,
       name,
       reg_time,
       vip,
       LEVEL,
       act_time
FROM parse_info
WHERE ds>='20161010'
  AND ds <= '20161017'
'''
chengyuan_df,info_df = hqls_to_dfs([chengyuan_sql, info_sql])

# 所有工会的成员
chengyuan_dfs = []
for _, row in chengyuan_df.iterrows():
    for player in list(eval(row['player'])):
        chengyuan_data = DataFrame({'ds': row['ds'], 'ass_id': row['ass_id'], 'name': row['name'],'player': [player]})
        chengyuan_dfs.append(chengyuan_data)
chengyuan_result_df = pd.concat(chengyuan_dfs)

# result = chengyuan_result_df.loc[chengyuan_result_df.ass_id == 'tw2071411']

# for date in date_range('20161010','20161014'):
#     print date


chengyuan_result_df.to_csv('/Users/kaiqigu/Downloads/Excel/chengyuan.txt',index=False)

df = chengyuan_result_df.groupby(['ass_id','player']).agg({'name':'count','ds':'max'}).reset_index()

df.to_csv('/Users/kaiqigu/Downloads/Excel/df.txt',index=False)




