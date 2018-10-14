#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 三国 滚服数据
create_date : 2016.05.06
'''
import settings_dev
import pandas as pd
from utils import ds_add, hql_to_df, update_mysql, get_config, hqls_to_dfs,date_range


def dis_act_roll_server_info(date):
    # 当日用户信息
    raw_info_sql = "select user_id,device from raw_info where ds = '{0}'".format(
        date)
    # 当日之前的用户信息
    mid_info_all_sql = "select  user_id,device,level from mid_info_all where ds = '{0}'".format(
        ds_add(date, -1))
    # 当日注册用户信息
    register_sql = "select  user_id from  raw_registeruser where ds = '{0}'".format(
        date)
    # 当日充值用户信息
    raw_pay_sql = "select user_id,order_money from raw_paylog where ds = '{0}' and platform_2 != 'admin_test' ".format(
        date)

    raw_info_df, mid_info_all_df, register_df, raw_pay_df = hqls_to_dfs(
        [raw_info_sql, mid_info_all_sql, register_sql, raw_pay_sql])

    # 当日活跃设备数
    active_df = raw_info_df.drop_duplicates(['device']).count()
    active_df = pd.DataFrame([active_df])
    active_df['activenum'] = active_df['device']
    active_df['ds'] = date
    columns = ['ds', 'activenum']
    active_df = active_df[columns]

    # 新增设备数
    raw_info_df['is_new_device'] = raw_info_df['device'].isin(
        mid_info_all_df.device.values)
    new_dev_df = raw_info_df[~raw_info_df['is_new_device']]
    new_dev_df = new_dev_df.drop_duplicates(['device']).count()
    new_dev_df = pd.DataFrame([new_dev_df])
    new_dev_df['new_dev'] = new_dev_df['device']
    new_dev_df['ds'] = date
    columns = ['ds', 'new_dev']
    new_dev_df = new_dev_df[columns]

    # 老服设备数
    raw_info_df['is_new_user'] = raw_info_df['user_id'].isin(
        register_df.user_id.values)
    old_dev_df = raw_info_df[~raw_info_df['is_new_user']]
    old_dev_df = old_dev_df.drop_duplicates(['device']).count()
    old_dev_df = pd.DataFrame([old_dev_df])
    old_dev_df['old_dev'] = old_dev_df['device']
    old_dev_df['ds'] = date
    columns = ['ds', 'old_dev']
    old_dev_df = old_dev_df[columns]

    # 滚服玩家信息
    # 设备：wifi02:00:00:00:00:00 为设备取不到时 赋的默认值
    roll_user_df = raw_info_df[raw_info_df['is_new_user']]
    mid_info_all_df['is_roll_dev'] = mid_info_all_df['device'].isin(
        roll_user_df.device.values)
    roll_user_df = mid_info_all_df[mid_info_all_df['is_roll_dev']]
    roll_user_df = roll_user_df[roll_user_df['level'] > 10]
    roll_user_df = roll_user_df[roll_user_df['device'] !=
                                'wifi02:00:00:00:00:00']

    # 滚服设备数
    # 设备：wifi02:00:00:00:00:00 为设备取不到时 赋的默认值
    roll_df = roll_user_df.drop_duplicates(['device']).count()
    roll_df = pd.DataFrame([roll_df])
    roll_df['roll_num'] = roll_df['device']
    roll_df['ds'] = date
    columns = ['ds', 'roll_num']
    roll_df = roll_df[columns]

    # 滚服玩家新充值总额
    raw_info_df['is_roll_dev'] = raw_info_df['device'].isin(
        roll_user_df.device.values)
    roll_pay_df = raw_info_df[raw_info_df['is_roll_dev']]
    raw_pay_df['is_roll_user'] = raw_pay_df['user_id'].isin(
        roll_pay_df.user_id.values)
    roll_pay_df = raw_pay_df.loc[raw_pay_df['is_roll_user']].copy()
    roll_pay_df['ds'] = date
    roll_pay_df = roll_pay_df.groupby('ds').sum().reset_index()
    #print roll_pay_df
    name =  pd.DataFrame(roll_pay_df).icol(1).name
    #print name
    if name != 'order_money':
        roll_pay_df= pd.DataFrame(columns=['ds', 'is_roll_user', 'order_money'])
    #print roll_pay_df

    roll_pay_df['roll_pay_num'] = roll_pay_df['order_money']
    columns = ['ds', 'roll_pay_num' ]
    roll_pay_df = roll_pay_df[columns]

    # 新增设备充值金额
    new_dev_pay_df = raw_info_df[~raw_info_df['is_new_device']]
    raw_pay_df['is_new_dev'] = raw_pay_df['user_id'].isin(
        new_dev_pay_df.user_id.values)
    new_dev_pay_df = raw_pay_df.loc[raw_pay_df['is_new_dev']].copy()
    new_dev_pay_df['ds'] = date
    new_dev_pay_df = new_dev_pay_df.groupby('ds').sum().reset_index()
    #print new_dev_pay_df
    name_2 = pd.DataFrame(new_dev_pay_df).icol(1).name
    #print name_2
    if name_2 != 'order_money':
        new_dev_pay_df = pd.DataFrame(columns=['ds', 'is_roll_user', 'order_money','is_new_dev'])
    #print new_dev_pay_df

    new_dev_pay_df['new_pay_num'] = new_dev_pay_df['order_money']
    columns = ['ds', 'new_pay_num']
    new_dev_pay_df = new_dev_pay_df[columns]

    # 老服玩家充值金额
    raw_info_df['is_reg_user'] = raw_info_df['user_id'].isin(
        register_df.user_id.values)
    olddev_userdf = raw_info_df[~raw_info_df['is_reg_user']]
    raw_info_df['is_old_dev'] = raw_info_df['device'].isin(
        olddev_userdf.device.values)
    olddev_userdf = raw_info_df[raw_info_df['is_old_dev']]
    raw_pay_df['is_old_dev'] = raw_pay_df['user_id'].isin(
        olddev_userdf.user_id.values)
    olddev_userdf = raw_pay_df.loc[raw_pay_df['is_old_dev']].copy()
    olddev_userdf['ds'] = date
    olddev_userdf = olddev_userdf.groupby('ds').sum().reset_index()
    #print '==============',olddev_userdf
    name_3 = pd.DataFrame(olddev_userdf).icol(1).name
    #print name_3
    if name_3 != 'order_money':
        olddev_userdf = pd.DataFrame(columns=['ds', 'is_roll_user', 'order_money', 'is_new_dev','is_old_dev'])
    #print olddev_userdf
    olddev_userdf['old_pay_num'] = olddev_userdf['order_money']
    columns = ['ds', 'old_pay_num']
    olddev_userdf = olddev_userdf[columns]

    # 当日充值总额
    # ds,order_money
    raw_pay_df['ds'] = date
    pay_df = raw_pay_df.groupby('ds').sum().reset_index()
    #print pay_df
    name_4 = pd.DataFrame(pay_df).icol(1).name
    #print name_4
    if name_4 != 'order_money':
        pay_df = pd.DataFrame(columns=['ds', 'is_roll_user', 'order_money', 'is_new_dev', 'is_old_dev'])
    #print pay_df
    pay_df['pay_num'] = pay_df['order_money']

    columns = ['ds', 'pay_num']
    pay_df = pay_df[columns]

    roll_server_data = (active_df.merge(new_dev_df,
                                        on=['ds'],
                                        how='outer').merge(old_dev_df,
                                                           on=['ds'],
                                                           how='outer')
                        .merge(roll_df,
                               on=['ds'],
                               how='outer').merge(roll_pay_df,
                                                  on=['ds'],
                                                  how='outer')
                        .merge(new_dev_pay_df,
                               on=['ds'],
                               how='outer').merge(olddev_userdf,
                                                  on=['ds'],
                                                  how='outer')
                        .merge(pay_df,
                               on=['ds'],
                               how='outer'))

    columns = ['ds', 'activenum', 'new_dev', 'roll_num', 'old_dev', 'pay_num',
               'new_pay_num', 'roll_pay_num', 'old_pay_num']
    roll_server_data = roll_server_data[columns]
    table = 'dis_act_roll_server_info'
    #print roll_server_data
    roll_server_data = pd.DataFrame(roll_server_data).fillna(0)

    # 更新MySQL表
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, roll_server_data, del_sql)

    return roll_server_data



if __name__ == '__main__':
    # settings_dev.set_env('sanguo_ks')
    # date = '20160426'
    # mm = dis_act_roll_server_info(date)
    for platform in ['sanguo_tt',]:
          settings_dev.set_env(platform)
          for date in date_range('20170407','20170417'):
                result = dis_act_roll_server_info(date)
    print "end"
