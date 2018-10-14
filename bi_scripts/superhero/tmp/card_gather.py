#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 卡牌集换
'''
from utils import hql_to_df, ds_add
import pandas as pd
import datetime
import settings
from pandas import DataFrame

settings.set_env('superhero_bi')
sql = '''
SELECT stmp,
       uid,
       args
FROM raw_action_log
WHERE ds in('20160907','20160908')
  AND action = 'card_collection.exchange'
  and substr(uid,1,1) = 'g'
'''
tt_df = hql_to_df(sql)
dfs = []
for _, row in tt_df.iterrows():
    # print row['uid']
    # print eval(row['args'])['exchange_id']
    data = DataFrame({'stmp':[row['stmp']],'uid':row['uid'],'exchange_id':eval(row['args'])['exchange_id']})
    dfs.append(data)

result_df = pd.concat(dfs)
result_df = result_df.sort_index(by=['stmp','uid','exchange_id'])

columns = ['stmp','uid','exchange_id']
result_df = result_df[columns]

result_df.to_excel('/Users/kaiqigu/Downloads/Excel/card_gather.xlsx')

select * from
(
select distinct ds_num from test_a
)a join
(
select distinct ds_num from test_a
)b on 1=1






