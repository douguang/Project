#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
from utils import hql_to_df, ds_add
import pandas as pd
import datetime
import settings
from pandas import DataFrame

settings.set_env('superhero_qiku')
sql = '''
SELECT stmp,
       uid,
       args
FROM raw_action_log
WHERE ds = '20160904'
  -- AND action = 'cards.card_rebirth'
  AND action = 'cards.super_evolution'
  -- and substr(uid,1,1) = 'a'
'''
tt_df = hql_to_df(sql)
dfs = []
for _, row in tt_df.iterrows():
    # print row['uid']
    # print [eval(row['args'])['major'][0].split('-')[0]]
    # data = DataFrame({'stmp':row['stmp'],'uid':row['uid'],'card_id':[eval(row['args'])['card_id'][0].split('-')[0]]})
    data = DataFrame({'stmp':row['stmp'],'uid':row['uid'],'card_id':[eval(row['args'])['major'][0].split('-')[0]]})
    dfs.append(data)

result_df = pd.concat(dfs)
result_df = result_df.sort_index(by=['uid','card_id'])

columns = ['stmp','uid','card_id']
result_df = result_df[columns]

# result_df.to_excel('/Users/kaiqigu/Downloads/Excel/card_rebirth.xlsx')
result_df.to_excel('/Users/kaiqigu/Downloads/Excel/card_evo_qiku.xlsx')

ALTER TABLE superhero_self_en.raw_action_log CHANGE pre_star stmp float;
