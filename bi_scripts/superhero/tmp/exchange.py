#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 限时兑换数据
'''
from utils import hql_to_df, ds_add
import pandas as pd
import settings
from pandas import DataFrame


settings.set_env('superhero_vt')
date = '20170514'

sql = '''
SELECT uid,
       args
FROM raw_action_log
WHERE action = 'omni_exchange.omni_exchange'
  AND ds = '{date}'
'''.format(date=date)
tt_df = hql_to_df(sql)
tt_df =
result_df['uid'] = result_df['uid'].astype('string')
# dfs = []
# for _, row in tt_df.iterrows():
#     # print row['uid']
#     # print eval(row['args'])['id'][0]
#     data = DataFrame({'uid': row['uid'], 'id': [eval(row['args'])['id'][0]]})
#     dfs.append(data)

# result_df = pd.concat(dfs)

# result_df['id'] = result_df['id'].astype('int')
# # result_df['id'] = result_df['id'].map(lambda s: int(s))
# id_list = [7908]
# result = result_df[result_df['id'].isin(id_list)]

# result_df['is_bao'] = result_df['id'].isin(id_list)
# result = result_df[result_df['is_bao']]
# del result['is_bao']

# # 兑换人数
# num_df = result.drop_duplicates(['id', 'uid'])
# num_df = num_df.groupby('id').count().reset_index().rename(
#     columns={'uid': 'num'})

# # 兑换次数
# times_df = result.groupby('id').count().reset_index().rename(
#     columns={'uid': 'times'})

# result_df = num_df.merge(times_df, on='id')

# result_df.to_excel('/Users/kaiqigu/Downloads/Excel/omni_exchange.xlsx')
