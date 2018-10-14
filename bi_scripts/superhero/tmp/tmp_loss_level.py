#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 流失等级分布
'''
from utils import hql_to_df, ds_add
import pandas as pd
import settings
from pandas import DataFrame


settings.set_env('superhero_bi')
sql = '''
SELECT uid,
       level
FROM raw_info
WHERE ds='20170112'
  AND uid NOT IN
    (SELECT uid
     FROM raw_info
     WHERE ds ='20170113')
'''
df = hql_to_df(sql)

result = df.groupby('level').count().reset_index()

result.to_excel('/Users/kaiqigu/Documents/Excel/loss_level.xlsx')

# aa = df.merge(act_df,on='uid',how='left').fillna(0)
# aa[aa.args == 0]

