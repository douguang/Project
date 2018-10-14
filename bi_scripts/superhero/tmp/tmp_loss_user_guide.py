#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 新手指引步骤
'''
import settings_dev
from utils import hql_to_df, ds_add
from pandas import DataFrame
import pandas as pd

settings_dev.set_env('superhero_tw')
date = '20170317'

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
   WHERE ds = '{date}'
     AND action = 'user.guide')a
WHERE rn = 1
 AND uid NOT IN
   (SELECT uid
    FROM raw_info
    WHERE ds ='{date_last}')
 '''.format(date=date, date_last=ds_add(date, 1))
act_df = hql_to_df(act_sql)
reg_sql = '''
SELECT uid
FROM raw_info
WHERE regexp_replace(substr(create_time,1,10),'-','') = '{date}'
'''.format(date=date)
reg_df = hql_to_df(reg_sql)
result = act_df.merge(reg_df, on='uid')

dfs = []
for _, row in result.iterrows():
    # print row['uid']
    # print eval(row['args'])['id'][0]
    data = DataFrame({'uid': row['uid'],
                      'server': row['server'],
                      'guide_id': [eval(row['args'])['guide_id'][0]]})
    dfs.append(data)

result_df = pd.concat(dfs)
result_df['guide_id'] = result_df['guide_id'].map(lambda s: int(s))
result = result_df.groupby('uid').max().reset_index()

guide_id_data = result.groupby(
    ['guide_id', 'server']).count().uid.reset_index().rename(
        columns={'uid': 'guide_id_user_num'})

guide_id_data.to_excel('/Users/kaiqigu/Documents/Excel/guide_id_data.xlsx')
