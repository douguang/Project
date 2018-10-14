#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 充值用户分布（分渠道）
'''
from utils import update_mysql, hqls_to_dfs, date_range
import settings_dev


def dis_pay_platform(date):
    table = 'dis_pay_platform'
    print table
    # 新增用户清单
    new_user_sql = '''
    select distinct user_id from raw_registeruser where ds = '{0}'
    '''.format(date)

    # 活跃用户清单
    active_user_sql = '''
    select distinct user_id from raw_activeuser where ds = '{0}'
    '''.format(date)

    # 充值用户名单及金额
    order_money_sql = '''
    select user_id, sum(order_money) as daily_order_money
    from raw_paylog
    where ds = '{0}'
      and platform_2 != 'admin_test'
    group by user_id
    '''.format(date)

    # 老充值用户名单
    old_pay_user_sql = '''
    select distinct user_id
    from raw_paylog
    where ds < '{0}'
      and platform_2 != 'admin_test'
    '''.format(date)

    # 合并所有用户的信息
    user_info_sql = '''
    select * from
    (
      select user_id, platform, vip_exp
      from mid_info_all
      where ds = '{0}'
    ) t3
    left semi join
    (
      select distinct user_id from (
        select distinct user_id from raw_activeuser where ds = '{0}'
        union all
        select distinct user_id from raw_registeruser where ds = '{0}'
        union all
        select distinct user_id from raw_paylog where ds = '{0}'
      ) t1
    ) t2
    on t2.user_id = t3.user_id
    '''.format(date)

    # 多线程同时执行hive SQL命令，加快速度
    new_user_df, active_user_df, order_money_df, old_pay_user_df, user_info_df = hqls_to_dfs(
        [new_user_sql, active_user_sql, order_money_sql, old_pay_user_sql,
         user_info_sql])
    # print user_info_sql
    # print user_info_df
    # 数据处理
    user_info_df.columns = ['user_id', 'platform', 'vip_exp']
    # 新用户
    user_info_df['is_new_user'] = user_info_df['user_id'].isin(
        new_user_df.user_id.values)
    user_info_df['is_old_payuser'] = user_info_df['user_id'].isin(
        old_pay_user_df.user_id.values)
    user_info_with_pay_df = user_info_df.merge(order_money_df,
                                               on=['user_id'],
                                               how='left')
    user_info_with_pay_df.fillna(0, inplace=True)

    user_info_with_pay_df['dau'] = 1
    # 充值人数
    user_info_with_pay_df[
        'is_payed'] = user_info_with_pay_df.daily_order_money != 0
    # 新增用户充值人数
    user_info_with_pay_df['is_new_and_payed'] = user_info_with_pay_df.apply(
        lambda row: row['is_new_user'] and row['is_payed'], axis=1)
    user_info_with_pay_df['new_and_payed_amount'] = user_info_with_pay_df[
        'is_new_and_payed'] * user_info_with_pay_df['daily_order_money']
    # 非新增用户首次充值人数
    user_info_with_pay_df['is_old_user_and_new_payuser'] = user_info_with_pay_df.apply(
        lambda row: not row['is_new_user'] and not row[
            'is_old_payuser'] and row['is_payed'],
        axis=1)
    user_info_with_pay_df[
        'old_user_and_new_payuser_amount'] = user_info_with_pay_df[
            'is_old_user_and_new_payuser'] * user_info_with_pay_df[
                'daily_order_money']
    # 老充值用户充值人数
    user_info_with_pay_df[
        'is_old_payuser_and_payed'] = user_info_with_pay_df.apply(
            lambda row: row['is_old_payuser'] and row['is_payed'],
            axis=1)
    user_info_with_pay_df[
        'old_payuser_and_payed_amount'] = user_info_with_pay_df[
            'is_old_payuser_and_payed'] * user_info_with_pay_df[
                'daily_order_money']

    # 按渠道汇总数据
    pay_platform_df = user_info_with_pay_df.groupby('platform').sum(
    ).reset_index()
    pay_platform_df['date'] = date
    del pay_platform_df['is_old_payuser']

    # 更新MySQL表
    del_sql = 'delete from {0} where date="{1}"'.format(table, date)
    update_mysql(table, pay_platform_df, del_sql)
if __name__ == '__main__':
    # for platform in ['sanguo_tw', 'sanguo_kr', 'sanguo_ks']:
    #     settings_dev.set_env(platform)
    #     for date in date_range('20161110', '20161121'):
    #         dis_pay_platform(date)
    for platform in ['qiling_ios', 'qiling_ks', 'qiling_tx']:
        settings_dev.set_env(platform)
        for date in date_range('20161229', '20170118'):
            dis_pay_platform(date)
    print 'end'
