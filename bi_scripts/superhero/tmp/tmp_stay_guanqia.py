#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 停留关卡
'''
import settings_dev
import pandas as pd
from pandas import DataFrame
from utils import hqls_to_dfs

settings_dev.set_env('superhero_tw')

info_sql = '''
SELECT uid,
       CASE WHEN vip = 0 THEN 0 ELSE 1 END vip
FROM
  (SELECT uid,
          max(vip_level) vip
   FROM raw_info
   WHERE ds>='20170316'
     AND ds <='20170321'
   GROUP BY uid)a
'''
act_sql = '''
SELECT uid,
       reverse(substring(reverse(uid), 8)) AS server,
       args
FROM
  (SELECT uid,
          stmp,
          dense_rank() over(partition BY uid
                            ORDER BY stmp DESC) AS rn,
          args
   FROM raw_action_log
   WHERE ds>='20170316'
     AND ds <='20170321'
     AND action = 'private_city.recapture')a
WHERE rn = 1
'''
info_df,act_df = hqls_to_dfs([info_sql, act_sql])

dfs = []
for _, row in act_df.iterrows():
    data = DataFrame({'uid': row['uid'],
                      'server': row['server'],
                      'city': [eval(row['args'])['city'][0]]})
    dfs.append(data)
result_df = pd.concat(dfs)
result_df = result_df.merge(info_df,on=['uid'])
result = result_df.groupby('uid').max().reset_index()

city_data = result.groupby(
    ['city', 'server', 'vip']).count().uid.reset_index().rename(
        columns={'uid': 'city_user_num'})

city_data.to_excel('/Users/kaiqigu/Documents/Excel/city_data.xlsx')
