#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 聚宝盆活动
Time        : 2017.03.27
'''
from utils import hql_to_df
# import pandas as pd
import settings_dev
# from pandas import DataFrame

settings_dev.set_env('superhero_bi')
bowl_sql = '''
SELECT uid,
       -- reverse(substr(reverse(uid),8)) as server,
       substr(uid,1,1) as pt,
       args
FROM raw_action_log
WHERE ds='20170325'
  AND action = 'bowl.choice'
'''
bowl_df = hql_to_df(bowl_sql)

bowl_df['num'] = bowl_df['args'].map(lambda s: eval(str(s))['num'][0])
bowl_df['sort'] = bowl_df['args'].map(lambda s: eval(str(s))['sort'][0])
bowl_df['num'] = bowl_df['num'].astype('int')
bowl_df['num'] = bowl_df['num'].astype('int')
times_df = bowl_df.groupby(
    ['sort', 'pt']).sum().reset_index().rename(
        columns={'num': 'times'})
num_df = bowl_df.drop_duplicates(['uid', 'sort', 'pt']).groupby(
    ['sort', 'pt']).count().reset_index()
result_df = times_df.merge(num_df, on=['sort', 'pt'])
# result_df.to_excel('/Users/kaiqigu/Documents/Excel/result_df.xlsx')
