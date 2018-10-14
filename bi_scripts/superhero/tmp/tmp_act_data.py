#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 未活跃玩家（近一个月活跃的用户中，在过年期间未活跃的玩家）
Date        : 2017-02-06
'''
from utils import hqls_to_dfs, get_rank, hql_to_df, date_range
import pandas as pd
import settings_dev
from pandas import DataFrame

settings_dev.set_env('superhero_qiku')
sql = '''
SELECT ds, uid
FROM raw_info
WHERE ds >='20170105'
  AND ds<='20170205'
  and substr(uid,1,1) ='q'
'''
df = hql_to_df(sql)

aa = df.drop_duplicates('uid')

for date in date_range('20170127','20170205'):
    print date
    act_df = df[df.ds == date]
    aa['is_act'] = aa['uid'].isin(act_df.uid.values)
    act_result = aa[~aa['is_act']]
    act_result.to_excel('/Users/kaiqigu/Documents/Excel/{date}.xlsx'.format(date=date))





