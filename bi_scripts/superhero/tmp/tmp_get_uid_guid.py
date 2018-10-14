#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
import pandas as pd
from utils import hql_to_df
import settings_dev

settings_dev.set_env('superhero_bi')
# sql = '''
# select uid from mid_info_all where ds ='20170118'
# '''
# mid_info = hql_to_df(sql)
reg_sql = '''
SELECT reg_ds,
       uid
FROM
  (SELECT ds AS reg_ds,
          uid
   FROM raw_reg
   WHERE ds = '20170117'
   and substr(uid,1,1) = 'g')a LEFT semi
JOIN
  (SELECT ds ,
          uid
   FROM raw_info
   WHERE ds = '20170117'
   and substr(uid,1,1) = 'g')b ON a.reg_ds = b.ds
AND a.uid = b.uid
'''
reg_df = hql_to_df(reg_sql)

df = pd.read_table("/Users/kaiqigu/Documents/scripts/nginx_log/log_data")
df_16_data = df[(df.stmp >= '2017-01-16 00:00:00') & (df.stmp <= '2017-01-16 23:59:59') & (df.plat == 'g')]
df_17_data = df[(df.stmp >= '2017-01-17 00:00:00') & (df.stmp <= '2017-01-17 23:59:59') & (df.plat == 'g')]
df_18_data = df[(df.stmp >= '2017-01-18 00:00:00') & (df.stmp <= '2017-01-18 23:59:59') & (df.plat == 'g')]
df_19_data = df[(df.stmp >= '2017-01-19 00:00:00') & (df.stmp <= '2017-01-19 23:59:59') & (df.plat == 'g')]

# df_16_data['is_reg'] = df_16_data['uid'].isin(mid_info.uid.values)
# # nginx解析获得的注册用户数：1921，数据平台记录的注册用户数：1499, 相差用户：422个
# df_16_reg = df_16_data[~df_16_data['is_reg']]
# df_16_reg = df_16_reg.sort_values('stmp',ascending=False)
# all_guid = df_16_reg.drop_duplicates('uid').groupby('method').count().stmp.reset_index()
# # nginx比数据平台多记录的的用户的新手指引情况
# df_16_reg['is_duo'] = df_16_reg['uid'].isin(reg_df.uid.values)
# duo_df = df_16_reg[~df_16_reg['is_duo']]
# duo_guid = duo_df.drop_duplicates('uid').groupby('method').count().stmp.reset_index()

# duo_guid.to_excel('/Users/kaiqigu/Documents/Excel/duo_guid.xlsx')

guid_list = ['platform_access'
,'get_user_server_list'
,'loading'
,'all_config'
,'new_user'
,'mark_user_login'
,'cards.open'
,'private_city.open'
,'user.rename']
# ,'user.guide']
# duo_df['is_guid'] = duo_df['method'].isin(guid_list)
# cc = duo_df[duo_df['is_guid']]
# ee = duo_df[~duo_df['is_guid']]

# ee.drop_duplicates('uid').count()   #419
# mm = ee[ee.method.values == 'user.guide'] # 283
# ee['is_userguid'] = ee['uid'].isin(ww.uid.values)
# rr = ee[~ee['is_userguid']]
# rr.drop_duplicates('uid').count()   #136

df_16_data_df = df_16_data.merge(reg_df,on='uid')
df_16_data_df['is_guid'] = df_16_data_df['method'].isin(guid_list)
cc = df_16_data_df[df_16_data_df['is_guid']]
mm = df_16_data_df[df_16_data_df.method == 'user.guide']
ss = cc.groupby(['uid','method']).count().stmp.reset_index().groupby('method').count().uid.reset_index()
ss.to_excel('/Users/kaiqigu/Documents/Excel/ss.xlsx')


df_17_data_df = df_17_data.merge(reg_df,on='uid')
df_17_data_df['is_guid'] = df_17_data_df['method'].isin(guid_list)
cc = df_17_data_df[df_17_data_df['is_guid']]
mm = df_17_data_df[df_17_data_df.method == 'user.guide']
ss = cc.groupby(['uid','method']).count().stmp.reset_index().groupby('method').count().uid.reset_index()
ss.to_excel('/Users/kaiqigu/Documents/Excel/ss.xlsx')

# cc['is_userguid'] = cc['uid'].isin(mm.uid.values)
# ee = cc[~cc['is_userguid']]
# ee['is_new'] = ee['uid'].isin(mid_info.uid.values)
# ee = ee[~ee['is_new']]
# ee.drop_duplicates('uid').count() #128
# ss = ee.drop_duplicates('uid').groupby('method').count().uid.reset_index()
# ss.to_excel('/Users/kaiqigu/Documents/Excel/ss.xlsx')


# df_17_data['is_guid'] = df_17_data['method'].isin(guid_list)
# cc = df_17_data[df_17_data['is_guid']]
# mm = df_17_data[df_17_data.method == 'user.guide']
# cc['is_userguid'] = cc['uid'].isin(mm.uid.values)
# ee = cc[~cc['is_userguid']]
# ee['is_new'] = ee['uid'].isin(mid_info.uid.values)
# ee = ee[~ee['is_new']]
# ee.drop_duplicates('uid').count() #128

# df_18_data['is_guid'] = df_18_data['method'].isin(guid_list)
# cc = df_18_data[df_18_data['is_guid']]
# mm = df_18_data[df_18_data.method == 'user.guide']
# cc['is_userguid'] = cc['uid'].isin(mm.uid.values)
# ee = cc[~cc['is_userguid']]
# ee['is_new'] = ee['uid'].isin(mid_info.uid.values)
# ee = ee[~ee['is_new']]
# ee.drop_duplicates('uid').count() #128
# ss = ee.drop_duplicates('uid').groupby('method').count().uid.reset_index()
# ss.to_excel('/Users/kaiqigu/Documents/Excel/ss.xlsx')


# df_16_reg['is_guid'] = df_16_reg['method'].isin(guid_list)
# cc = df_16_reg[df_16_reg['is_guid']]
# cc.groupby(['uid','method']).count().reset_index().groupby('method').count().uid.reset_index().sum()

# cc.groupby(['uid','method']).count().reset_index().groupby('uid').method.count().reset_index().groupby('method').count().reset_index()

# mm = duo_df.drop_duplicates(['uid','method']).groupby('uid').count().stmp.reset_index()

# action_sql='''
# SELECT uid,
#        args
# FROM raw_action_log
# WHERE ds ='20170116'
#   AND action = 'user.guide'
#   AND substr(uid,1,1) = 'g'
# '''
# action_df = hql_to_df(action_sql)
# mm['is_action'] = mm['uid'].isin(action_df.uid.values)

# info_sql = '''
# select uid,create_time from raw_info where ds ='20170116'
# '''
# info_df = hql_to_df(info_sql)
# ss = mm.drop_duplicates('uid').merge(info_df,on='uid')




