#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 限时兑换（去水）
'''
from utils import hql_to_df, ds_add
import pandas as pd
import settings_dev
from pandas import DataFrame


settings_dev.set_env('dancer_tx_beta')
# 获取水军数据
df = pd.read_excel("/Users/kaiqigu/Downloads/Excel/water_uid.xlsx")
first_pay_sql = '''
SELECT ds,
       user_id,
       server,
       a_typ,
       a_tar args
FROM mid_actionlog
WHERE ds >='20160913'
and ds <='20160920'
  AND a_typ = 'server_exchange.server_omni_exchange'
'''
first_pay_df = hql_to_df(first_pay_sql)

first_pay_df['is_shui'] = first_pay_df['user_id'].isin(df.user_id.values)
result = first_pay_df[~first_pay_df['is_shui']]

dfs = []
for _, row in result.iterrows():
    args = eval(row['args'])
    if args.has_key('id'):
        data = DataFrame({'ds':row['ds'],'user_id':row['user_id'],'server':row['server'],'id':[args['id']]})
        dfs.append(data)

result_df = pd.concat(dfs)

# 兑换人数
num_df = result_df.drop_duplicates(['ds','id','server','user_id'])
num_df = num_df.groupby(['ds','id','server']).count().reset_index().rename(columns={'user_id':'num'})

# 兑换次数
times_df = result_df.groupby(['ds','id','server']).count().reset_index().rename(columns={'user_id':'times'})

result = num_df.merge(times_df,on= ['ds','id','server'])

result.to_excel('/Users/kaiqigu/Downloads/Excel/omni_exchange.xlsx')
