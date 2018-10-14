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

df = pd.read_table("/Users/kaiqigu/Documents/scripts/nginx_log/log_data")

df = df[(df.device_mark != 'wifi00:00:00:00:00:00') & (df.device_mark != 'wifi02:00:00:00:00:00')]

df_16_data = df[(df.stmp >= '2017-01-16 00:00:00') & (df.stmp <= '2017-01-16 23:59:59') & (df.pt == 'android')]
df_17_data = df[(df.stmp >= '2017-01-17 00:00:00') & (df.stmp <= '2017-01-17 23:59:59') & (df.pt == 'android')]
df_18_data = df[(df.stmp >= '2017-01-18 00:00:00') & (df.stmp <= '2017-01-18 23:59:59') & (df.pt == 'android')]
df_19_data = df[(df.stmp >= '2017-01-19 00:00:00') & (df.stmp <= '2017-01-19 23:59:59') & (df.pt == 'android')]

account_sql = '''
select account,device as device_mark from raw_info where ds = '20170116'
and regexp_replace(substr(create_time,1,10),'-','')='20170116'
and substr(uid,1,1)='g'
'''
account_df = hql_to_df(account_sql)

aa = df_16_data.merge(account_df,on='device_mark')
platform_access_dev = aa[aa.method == 'platform_access']
aa['is_dev'] = aa['device_mark'].isin(platform_access_dev.device_mark.values)
platform_access_data = aa[aa['is_dev']]

get_user_server_list = platform_access_data[platform_access_data.method == 'get_user_server_list']['device_mark'].tolist()
platform_access_data['is_dev'] = platform_access_data['device_mark'].isin(get_user_server_list)
get_user_server_data = platform_access_data[platform_access_data['is_dev']]

loading_list = get_user_server_data[get_user_server_data.method == 'loading']['device_mark'].tolist()
get_user_server_data['is_dev'] = get_user_server_data['device_mark'].isin(loading_list)
loading_data = get_user_server_data[get_user_server_data['is_dev']]

all_config_list = loading_data[loading_data.method == 'all_config']['device_mark'].tolist()
loading_data['is_dev'] = loading_data['device_mark'].isin(all_config_list)
all_config_data = loading_data[loading_data['is_dev']]

new_user_list = all_config_data[all_config_data.method == 'new_user']['device_mark'].tolist()
all_config_data['is_dev'] = all_config_data['device_mark'].isin(new_user_list)
new_user_data = all_config_data[all_config_data['is_dev']]

mark_user_login_list = new_user_data[new_user_data.method == 'mark_user_login']['device_mark'].tolist()
new_user_data['is_dev'] = new_user_data['device_mark'].isin(mark_user_login_list)
mark_user_login_data = new_user_data[new_user_data['is_dev']]

cards_open_list = mark_user_login_data[mark_user_login_data.method == 'cards.open']['device_mark'].tolist()
mark_user_login_data['is_dev'] = mark_user_login_data['device_mark'].isin(cards_open_list)
cards_open_data = mark_user_login_data[mark_user_login_data['is_dev']]

private_city_open_list = cards_open_data[cards_open_data.method == 'private_city.open']['device_mark'].tolist()
cards_open_data['is_dev'] = cards_open_data['device_mark'].isin(private_city_open_list)
private_city_open_list_data = cards_open_data[cards_open_data['is_dev']]

user_rename_list = private_city_open_list_data[private_city_open_list_data.method == 'user.rename']['device_mark'].tolist()
private_city_open_list_data['is_dev'] = private_city_open_list_data['device_mark'].isin(user_rename_list)
user_rename_data = private_city_open_list_data[private_city_open_list_data['is_dev']]

user_guide_list = user_rename_data[user_rename_data.method == 'user.guide']['device_mark'].tolist()
user_rename_data['is_dev'] = user_rename_data['device_mark'].isin(user_guide_list)
user_guide_data = user_rename_data[user_rename_data['is_dev']]



# ss = aa.groupby(['device_mark','method']).count().reset_index().groupby('method').count().device_mark.reset_index()
# ss.to_excel('/Users/kaiqigu/Documents/Excel/ss.xlsx')


# account_data = df_16_data.fillna(0)
# account_data = account_data[(account_data.account != 0) & (account_data.device_mark !=0)]
# account_data = account_data.drop_duplicates(['device_mark','account'])(columns=['device_mark','account'])
# columns = ['device_mark','account']
# account_data = account_data[columns]
# account_data['is_reg'] = account_data['account'].isin(account_df.account.values)
# new_account_data = account_data[account_data['is_reg']]

# del df_16_data['account']
# df_data = new_account_data.merge(df_16_data,on='device_mark')

# ss = df_data.groupby(['account','method']).count().reset_index().groupby('method').count().reset_index()

# ss.to_excel('/Users/kaiqigu/Documents/Excel/ss.xlsx')
