#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 10月2日团购玩家购买箱子数量
'''
from utils import hql_to_df, ds_add
import pandas as pd
import settings
from pandas import DataFrame


settings.set_env('superhero_qiku')

first_pay_sql = '''
SELECT uid,
       args
FROM raw_spendlog
WHERE ds ='20161002'
  AND goods_type = 'active.group_active_buy'
'''
first_pay_df = hql_to_df(first_pay_sql)

dfs = []
for _, row in first_pay_df.iterrows():
    args = eval(row['args'])
    if args.has_key('count'):
        data = DataFrame({'uid':row['uid'],'num':[args['count'][0]]})
        dfs.append(data)

result_df = pd.concat(dfs)
result_df['num'] = result_df['num'].map(lambda s: int(s))

result = result_df.groupby('uid').sum().reset_index()

result.to_excel('/Users/kaiqigu/Downloads/Excel/group_active_buy.xlsx')
