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
sql = '''
SELECT ds,
       uid,
       args
FROM row_action_log
WHERE ds >='20160919'
  AND ds <='20160924'
  AND action = 'god_field.buy_ship_ticket'
'''
tt_df = hql_to_df(sql)

dfs = []
for _, row in tt_df.iterrows():
    # print row['uid']
    # print eval(row['args'])['id'][0]
    data = DataFrame({'ds':row['ds'],'uid':row['uid'],'id':[eval(row['args'])['id']]})
    dfs.append(data)

result_df = pd.concat(dfs)

# 兑换人数
num_df = result_df.drop_duplicates(['ds','id','server','user_id'])
num_df = num_df.groupby(['ds','id','server']).count().reset_index().rename(columns={'user_id':'num'})

# 兑换次数
times_df = result_df.groupby(['ds','id','server']).count().reset_index().rename(columns={'user_id':'times'})

result = num_df.merge(times_df,on= ['ds','id','server'])

result.to_excel('/Users/kaiqigu/Downloads/Excel/omni_exchange.xlsx')




