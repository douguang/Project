#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 限时兑换数据
'''
from utils import hql_to_df, ds_add
import pandas as pd
import settings_dev
from pandas import DataFrame

settings_dev.set_env('dancer_ks_beta')

gs_sql = '''
select
distinct user_id
from raw_paylog where ds >= '20160913'
and platform_2 = 'admin_test'c
'''



sql = '''
SELECT ds,
       user_id,
       server,
       a_typ,
       a_tar args
FROM mid_actionlog
WHERE ds >='20160907'
  AND ds <='20160917'
  AND server >= 'tw0'
  AND server <= 'tw2'
  AND a_typ = 'server_exchange.server_omni_exchange'
'''
tt_df = hql_to_df(sql)

dfs = []
for _, row in tt_df.iterrows():
    # print row['uid']
    # print eval(row['args'])['id'][0]
    data = DataFrame({'ds':row['ds'],'user_id':row['user_id'],'server':row['server'],'id':[eval(row['args'])['id']]})
    dfs.append(data)

result_df = pd.concat(dfs)

# 兑换人数
num_df = result_df.drop_duplicates(['ds','id','server','user_id'])
num_df = num_df.groupby(['ds','id','server']).count().reset_index().rename(columns={'user_id':'num'})

# 兑换次数
times_df = result_df.groupby(['ds','id','server']).count().reset_index().rename(columns={'user_id':'times'})

result = num_df.merge(times_df,on= ['ds','id','server'])

result.to_excel('/Users/kaiqigu/Downloads/Excel/omni_exchange.xlsx')




