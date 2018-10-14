#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
import settings_dev
from utils import hql_to_df, update_mysql, ds_add, date_range
from pandas import DataFrame
import pandas as pd

settings_dev.set_env('dancer_tw')

act_sql = '''
SELECT user_id,
       account,
       a_tar
FROM mid_actionlog
WHERE ds ='20160907'
'''
reg_sql = '''
SELECT account,
       user_id,
       regexp_replace(substr(reg_time,1,10),'-','') reg_time
FROM mid_info_all
WHERE ds = '20160907'
'''
day_result = pd.read_table('/Users/kaiqigu/Downloads/Excel/1009uids')
reg_df = hql_to_df(reg_sql)
act_df = hql_to_df(act_sql)
dfs = []
for _, row in act_df.iterrows():
    user_id = row['user_id']
    args = eval(row['a_tar'])
    if args.has_key('appid'):
        appid = args['appid']
    else:
        appid = ' '
    data = DataFrame({'user_id':[user_id],'appid':[appid]})
    dfs.append(data)
result_df = pd.concat(dfs)

reg_ago_df = reg_df.loc[reg_df.reg_time != '20160907']
reg_df_data = reg_df.loc[reg_df.reg_time == '20160907']
reg_df_data['is_ago'] = reg_df_data['account'].isin(reg_ago_df.account.values)
reg_result_df = reg_df_data[~reg_df_data['is_ago']]
del reg_result_df['is_ago']

uid_info_df = result_df.groupby('user_id').max().appid.reset_index()
# uid_info_df = result_df.drop_duplicates(['appid','user_id'])

reg_result_df = reg_result_df.merge(uid_info_df,on = 'user_id',how = 'left')
day_result_df = day_result.merge(uid_info_df,on = 'user_id',how = 'left')

result = reg_result_df.merge(day_result_df,on =['user_id','appid'])

result_data = result.groupby('appid').count().user_id.reset_index()
day_ago_data = (reg_result_df.groupby('appid').count().user_id.reset_index()
.rename(columns = {'user_id':'ago_num'}))

data = result_data.merge(day_ago_data,on='appid',how='outer')

columns = [ 'appid','ago_num','user_id']
result_data = data[columns]

result_data.to_excel('/Users/kaiqigu/Downloads/Excel/reg_data.xlsx')
