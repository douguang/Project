#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
import pandas as pd
from utils import hql_to_df
import settings_dev
from pandas import DataFrame

settings_dev.set_env('superhero_bi')
# 前端日志
df = pd.read_table("/Users/kaiqigu/Documents/scripts/nginx_log/log")
nginx_df = pd.read_table("/Users/kaiqigu/Documents/scripts/nginx_log/log_data")

guid_list = ['platform_access', 'get_user_server_list', 'loading',
             'all_config', 'new_user', 'mark_user_login', 'cards.open',
             'private_city.open', 'user.rename', 'user.guide']


def get_day_data(source_df, col):
    '''获取一整天的数据'''
    return source_df[(source_df['stmp'] >= '2017-01-18 00:00:00') & (source_df[
        'stmp'] <= '2017-01-18 23:59:59')]


df_18_data = get_day_data(df, 'stmp')
nginx_18_data = get_day_data(nginx_df, 'stmp')

# 点击图标的用户，用户数
aa = df_18_data[df_18_data.user_name == 'nil']
nil_data = aa[(aa.newstep == 1) | (aa.newstep == '1')]
nil_num = nil_data.drop_duplicates('device_mark').count().device_mark
# 成功登陆sdk的用户
dau_data = df_18_data[(df_18_data.newstep == 2) | (df_18_data.newstep == '2')]

# # 点击图标的用户
# nil_data = df_18_data[(df_18_data.newstep == 1) & (df_18_data.user_name ==
#                                                    'nil')]
# 点击图标的用户数
# nil_num = nil_data.drop_duplicates('device_mark').count().stmp

# 成功登陆sdk的用户
# dau_data = df_18_data[(df_18_data.newstep == 2)]

# 点击图标的用户中成功登陆到sdk的用户：1656
dau_data['is_nil'] = dau_data['device_mark'].isin(nil_data.device_mark.values)
aa = dau_data[dau_data['is_nil']]
sdk_result = aa.drop_duplicates('device_mark')
sdk_num = aa.drop_duplicates('device_mark').count().stmp

# 登陆到sdk的用户 且 进入注册流程的用户
gg = sdk_result.merge(nginx_18_data, on='device_mark')
gg_num = gg.drop_duplicates('device_mark').count().device_mark
gg_data = gg.drop_duplicates('device_mark')

# 完成了新手引导的用户
gg['is_guid'] = gg['method'].isin(guid_list)
complete_guid_data = gg[~gg['is_guid']]
complete_guid_num = complete_guid_data.drop_duplicates('device_mark').count(
).device_mark

# 未完成新手引导的用户
gg['com_guid'] = gg['device_mark'].isin(complete_guid_data.device_mark.values)
notcom_guid_data = gg[~gg['com_guid']]
notcom_guid_data.drop_duplicates('method')
notcom_guid_data.drop_duplicates('device_mark')


def get_df(source_df, guide_name):
    ''' 查询参与接口的数据 和 未参与接口的数据'''
    guid_data = source_df[source_df.method == guide_name]
    # print guid_data.drop_duplicates('device_mark')
    # 查询参与该接口的设备数
    guid_num = guid_data.drop_duplicates('device_mark').count().method
    # 查询没有参与guide_name接口的数据
    source_df['is_com'] = source_df['device_mark'].isin(
        guid_data.device_mark.values)
    final_data = source_df[~source_df['is_com']]
    # 返回参与接口的设备数，未参与接口的数据
    return [guid_num, final_data]

# 新建角色
new_user_num, not_new_user_data = get_df(notcom_guid_data, 'new_user')
# 下载配置
all_config_num, not_all_config_data = get_df(not_new_user_data, 'all_config')
# 获取配置信息
loading_num, not_loading_data = get_df(not_all_config_data, 'loading')
# 获取用户ID
platform_access_num, not_platform_access_data = get_df(not_loading_data,
                                                       'platform_access')
# 获取服务器信息
server_list_num, not_server_list_data = get_df(not_platform_access_data,
                                               'get_user_server_list')
# # 获取服务器信息
# server_list_num, not_server_list_data = get_df(not_loading_data,
#                                                'get_user_server_list')
# # 获取用户ID
# platform_access_num, not_platform_access_data = get_df(not_server_list_data,
#                                                        'platform_access')

sql = '''
select create_time,fresh_time,account,device as device_mark
from mid_info_all where ds ='20170202'
'''
result_df = hql_to_df(sql)
# 登陆到sdk的用户中 老用户的数量
aa = gg_data.merge(result_df, on=['device_mark', 'account'])
aa_num = aa.drop_duplicates(['device_mark', 'account']).count().device_mark

data = DataFrame({'nil_num': [nil_num],
                  'sdk_num': [sdk_num],
                  'gg_num': [gg_num],
                  'complete_guid_num': [complete_guid_num],
                  'all_config_num': [all_config_num],
                  'loading_num': [loading_num],
                  'server_list_num': [server_list_num],
                  'platform_access_num': [platform_access_num],
                  'aa_num': [aa_num],
                  'new_user': [new_user_num]})
columns = ['nil_num', 'sdk_num', 'gg_num', 'complete_guid_num',
           'all_config_num', 'loading_num', 'server_list_num',
           'platform_access_num', 'aa_num', 'new_user']
data = data[columns]
data.to_excel('/Users/kaiqigu/Documents/Excel/20170203.xlsx')
