#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
import pandas as pd
from utils import hql_to_df
import settings_dev
from pandas import DataFrame

settings_dev.set_env('superhero_vt')
sql = '''
SELECT reverse(substr(reverse(uid),8)) AS server,
       uid,
       args
FROM raw_action_log
WHERE ds ='20170222'
  AND action = 'one_piece.exchange'
  AND reverse(substr(reverse(uid),8)) IN ('vncp',
                                          'vnck',
                                          'vncb')
'''
result = hql_to_df(sql)

dfs = []
for _, row in result.iterrows():
    data = DataFrame({'server': [row['server']],
                      'uid': row['uid'],
                      'id': eval(row['args'])['id']})
    dfs.append(data)

result_df = pd.concat(dfs)
one_piece_df = result_df.groupby(['server', 'id']).count().uid.reset_index()

one_piece_df.to_excel('/Users/kaiqigu/Documents/Excel/one_piece.xlsx')



