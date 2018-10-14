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
    return source_df[(source_df['stmp'] >= '2017-02-03 00:00:00') & (source_df[
        'stmp'] <= '2017-02-03 23:59:59')]


def get_dayago_data(source_df, col):
    '''获取一整天的数据'''
    return source_df[(source_df['stmp'] < '2017-02-03 00:00:00')]


df_18_data = get_day_data(df, 'stmp')
df_18ago_data = get_dayago_data(df, 'stmp')
nginx_18_data = get_day_data(nginx_df, 'stmp')

# 点击图标的用户，用户数
aa = df_18_data[df_18_data.user_name == 'nil']
nil_data = aa[(aa.newstep == 1) | (aa.newstep == '1')]
nil_data1 = nil_data.drop_duplicates('device_mark')
nil_num = nil_data.drop_duplicates('device_mark').count().device_mark

# 老用户数
sql = '''
select create_time,fresh_time,account,device as device_mark
from mid_info_all where ds ='20170202'
'''
result_df = hql_to_df(sql)
# # 登陆到sdk的用户中 老用户的数量
# aa = nil_data1.merge(result_df, on=['device_mark'])
# aa_num = aa.drop_duplicates(['device_mark']).count().device_mark

# 点击图标的设备中老设备的数量
dt = pd.concat([df_18ago_data, result_df])
old_dev = (nil_data1.merge(dt,
                           on=['device_mark']).drop_duplicates('device_mark'))
# nil_data1.merge(dt, on=['device_mark'])
aa_num = (nil_data1.merge(
    dt, on=['device_mark']).drop_duplicates('device_mark').count().device_mark)

print nil_num - aa_num

# 排除了老用户的设备数据
nil_data['is_old'] = nil_data['device_mark'].isin(old_dev.device_mark.values)
new_dev = nil_data[~nil_data['is_old']]
new_num = new_dev.drop_duplicates('device_mark').count().device_mark

# # 新设备中成功登陆sdk的设备
# sdk_data = df_18_data[(df_18_data.newstep == 2) | (df_18_data.newstep == '2')]
# sdk_data['is_new'] = sdk_data['device_mark'].isin(new_dev.device_mark.values)
# new_sdk_data = sdk_data[sdk_data['is_new']]

# data = new_dev.drop_duplicates(['platform', 'device_mark'])
# platform_df = data.groupby('platform').count().device_mark.reset_index()

# platform_df.to_excel('/Users/kaiqigu/Documents/Excel/platform.xlsx')

# 新设备中进入注册流程的用户
nginx_18_data['is_new'] = nginx_18_data['device_mark'].isin(
    new_dev.device_mark.values)
reg_data = nginx_18_data[nginx_18_data['is_new']]

# 完成了新手引导的用户
reg_data['is_guid'] = reg_data['method'].isin(guid_list)
complete_guid_data = reg_data[~reg_data['is_guid']]
complete_guid_num = complete_guid_data.drop_duplicates('device_mark').count(
).device_mark

# 未完成新手引导的用户
reg_data['com_guid'] = reg_data['device_mark'].isin(
    complete_guid_data.device_mark.values)
notcom_guid_data = reg_data[~reg_data['com_guid']]
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
data = DataFrame({'new_num': [new_num],
                  'server_list_num': [server_list_num],
                  'platform_access_num': [platform_access_num],
                  'loading_num': [loading_num],
                  'all_config_num': [all_config_num],
                  'new_user': [new_user_num]})
columns = ['new_num', 'server_list_num', 'platform_access_num', 'loading_num',
           'all_config_num', 'new_user']
data = data[columns]
data.to_excel('/Users/kaiqigu/Documents/Excel/20170118.xlsx')


# =======
config_df = not_new_user_data[not_new_user_data.method == 'all_config']
config_list = config_df.drop_duplicates('device_mark')['device_mark'].tolist()
new_dev_df = new_dev.drop_duplicates(['device_mark','platform'])
# new_dev_df = new_dev.groupby(['device_mark','platform']).stmp.count().reset_index()
new_dev_df['is_conf'] = new_dev_df['device_mark'].isin(config_list)
config_result = new_dev_df[new_dev_df['is_conf']]
config_result = config_result.fillna('None')
re = config_result.groupby('platform').count().is_conf.reset_index()
re.to_excel('/Users/kaiqigu/Documents/Excel/plat.xlsx')

# none_list = config_result[config_result['platform'] == 'None']['device_mark'].tolist()
# new_dev['is_none'] = new_dev['device_mark'].isin(none_list)
# none_df = new_dev[new_dev['is_none']]
# new_dev.groupby(['device_mark','platform']).stmp.count().reset_index()


