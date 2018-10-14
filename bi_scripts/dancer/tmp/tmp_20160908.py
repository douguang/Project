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
       level,
       account,
       a_tar
FROM mid_actionlog
WHERE ds ='20160908'
'''
reg_sql = '''
SELECT account,
       user_id
FROM parse_info
WHERE regexp_replace(substr(reg_time,1,10),'-','')= '20160908'
and ds = '20160908'
'''
info_sql = '''
SELECT account,
       user_id,
       level
FROM parse_info
WHERE ds = '20160908'
'''
reg_ago_sql = '''
SELECT account,
       user_id,
       regexp_replace(substr(reg_time,1,10),'-','') reg_time
FROM mid_info_all
WHERE ds = '20160907'
'''
reg_ago_df = hql_to_df(reg_ago_sql)
reg_ago_df = reg_ago_df.loc[reg_ago_df.reg_time < '20160907']
# mid_info_df = hql_to_df(mid_info_sql)
# day_result = pd.read_table('/Users/kaiqigu/Downloads/Excel/1009uids')
reg_df = hql_to_df(reg_sql)
info_df = hql_to_df(info_sql)
act_df = hql_to_df(act_sql)

# reg_df = reg_df.merge(mid_info_df,on = ['account','user_id'])
# dfs = []
user_id_list,level_list,appid_list = [],[],[]
for _, row in act_df.iterrows():
    # user_id = row['user_id']
    args = eval(row['a_tar'])
    if args.has_key('appid'):
        appid = args['appid']
    else:
        appid = ' '
    # data = DataFrame({'user_id':[user_id],'appid':[appid]})
    # dfs.append(data)
    user_id_list.append(row['user_id'])
    level_list.append(row['level'])
    appid_list.append(appid)

result_df = DataFrame({'user_id':user_id_list,'level':level_list,'appid':appid_list})
# result_df = pd.concat(dfs)

# reg_ago_df = reg_df.loc[reg_df.reg_time != '20160907']
# reg_df_data = reg_df.loc[reg_df.reg_time == '20160907']
# reg_df_data['is_ago'] = reg_df_data['account'].isin(reg_ago_df.account.values)
# reg_result_df = reg_df_data[~reg_df_data['is_ago']]
# del reg_result_df['is_ago']

uid_info_df = result_df.groupby(['user_id']).max().appid.reset_index()
# # uid_info_df = result_df.drop_duplicates(['appid','user_id'])

# reg_result_df = reg_df.merge(uid_info_df,on = 'user_id',how = 'left')
# day_result_df = info_df.merge(uid_info_df,on = 'user_id',how = 'left')
# reg_result_df['is_old'] = reg_result_df['account'].isin(reg_ago_df.account.values)
# day_result_df['is_old'] = day_result_df['account'].isin(reg_ago_df.account.values)
# reg_result_df = reg_result_df[~reg_result_df['is_old']]
# day_result_df = day_result_df[~day_result_df['is_old']]

# reg_result_df['uid_plat'] = reg_result_df['user_id'] + reg_result_df['appid']
# day_result_df['uid_plat'] = day_result_df['user_id'] + day_result_df['appid']

# reg_result_df['is_liu'] = reg_result_df['uid_plat'].isin(day_result_df.uid_plat.values)
# data = reg_result_df[~reg_result_df['is_liu']]


# # result = reg_result_df.merge(day_result_df,on =['user_id','appid'])

# data = (day_result_df.groupby('appid').count().user_id.reset_index())
# reg_data = (reg_result_df.groupby('appid').count().user_id.reset_index())

# data = data.merge(reg_data,on = 'appid')
# result_data = data.groupby(['appid','level']).count().user_id.reset_index()
# # day_ago_data = (reg_result_df.groupby('appid').count().user_id.reset_index()
# # .rename(columns = {'user_id':'ago_num'}))

# # data = result_data.merge(day_ago_data,on='appid',how='outer')

# # columns = [ 'appid','ago_num','user_id']
# # result_data = data[columns]

data.to_excel('/Users/kaiqigu/Downloads/Excel/reg_data.xlsx')
