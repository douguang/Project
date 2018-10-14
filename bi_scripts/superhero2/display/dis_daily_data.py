#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 超二 日常数据
create_date : 2016.07.17
Illustration: Dong Junshuang 2017.06.12重写，为保证各页面的用户数统计一致
'''
import settings_dev
import pandas as pd
import numpy as np
from utils import ds_add
from utils import hqls_to_dfs, hql_to_df
from utils import update_mysql
from utils import date_range
from superhero2.cfg import zichong_uids


act_days = [1, 7, 30]
rename_dic = {'d1_num': 'dau', 'd7_num': 'wau', 'd30_num': 'mau'}


def dis_daily_data(date):
    act_dates = [ds_add(date, 1 - act_day) for act_day in act_days]
    act_dates_dic = {ds_add(date, 1 - act_day): 'd%d_num' % act_day
                     for act_day in act_days}
    # 新增充值用户 - 分服
    new_order_sql = '''
    SELECT user_id
    FROM raw_paylog
    WHERE ds <= '{date}'
      AND platform <> 'admin_test'
      AND order_id NOT LIKE '%test%'
    GROUP BY user_id HAVING min(ds) = '{date}'
    '''.format(date=date)
    # 当日充值数据
    pay_sql = '''
        select count(t3.user_id) as pay_num, sum(t3.pay_money) as income, t3.platform, t3.ds from
        (select t1.user_id, t1.pay_money, t2.platform, t1.ds from (
          SELECT user_id, sum(order_rmb) AS pay_money, ds FROM raw_paylog WHERE ds = '{date}' AND platform <> 'admin_test'
          AND order_id NOT LIKE '%test%' GROUP BY user_id, ds) t1
            left join
            (select user_id, platform from parse_info where ds='{date}') t2
            on t1.user_id = t2.user_id
          ) t3 group by platform, ds
    '''.format(date=date)
    # 新注册用户user_id
    new_sql = '''
    SELECT ds,
           user_id
    FROM parse_info
    WHERE ds = '{date}'
      AND regexp_replace(substr(reg_time,1,10),'-','') = '{date}'
    '''.format(date=date)
    info_sql = '''
    SELECT DISTINCT ds,
                    user_id
    FROM parse_info
    WHERE ds >= '{start_date}'
      AND ds <= '{end_date}'
    '''.format(start_date=act_dates[-1],
               end_date=date)
    action_sql = '''
    SELECT DISTINCT ds,
                       user_id,
                       platform
       FROM parse_action_log
       WHERE ds >= '{start_date}'
         AND ds <= '{end_date}'
         AND platform <> 'None'
    '''.format(start_date=act_dates[-1],
               end_date=date)


    new_order_df = hql_to_df(new_order_sql)
    new_df = hql_to_df(new_sql)
    pay_df = hql_to_df(pay_sql)
    info_df = hql_to_df(info_sql)
    action_df = hql_to_df(action_sql)

    # 活跃用户数
    # 排除开服至今的gs数据
    new_order_df = new_order_df[~new_order_df['user_id'].isin(zichong_uids)]
    new_df = new_df[~new_df['user_id'].isin(zichong_uids)]
    # pay_df = pay_df[~pay_df['user_id'].isin(zichong_uids)]
    info_df = info_df[~info_df['user_id'].isin(zichong_uids)]
    action_df = action_df[~action_df['user_id'].isin(zichong_uids)]
    day_action_df = action_df[action_df.ds == date]
    # 匹配渠道
    new_order_df = new_order_df.merge(day_action_df,
                                      on='user_id',
                                      how='left').fillna('None')
    new_df = new_df.merge(day_action_df,
                          on=['ds', 'user_id'],
                          how='left').fillna('None')
    info_df = info_df.merge(action_df,
                            on=['ds', 'user_id'],
                            how='left').fillna('None')

    # 汇总数据
    # 新增充值用户
    new_order_result = new_order_df.groupby(
        ['ds', 'platform']).count().reset_index().rename(
            columns={'user_id': 'new_pay_num'})
    # 新注册用户
    new_df_result = new_df.groupby(
        ['ds', 'platform']).count().reset_index().rename(
            columns={'user_id': 'reg_user_num'})
    # 充值总额
    pay_df_result = pay_df.rename(columns={'user_id': 'pay_num', 'pay_money': 'income'})

    # 活跃用户数
    act_list = []
    for i in act_dates:
        act_result = info_df.loc[(info_df.ds >= i) & (info_df.ds <= date)]
        act_result.loc[:, ['ds']] = i
        act_result = act_result.drop_duplicates(['user_id'])
        act_list.append(act_result)
    act_num_df = pd.concat(act_list)
    act_num_df['act'] = 1
    act_finnum_df = (
        act_num_df.pivot_table('act', ['user_id', 'platform'], 'ds')
        .reset_index().groupby('platform').sum().reset_index()
        .rename(columns=act_dates_dic))
    act_finnum_df['ds'] = date

    # 最终结果
    result_df = (new_order_result.merge(
        act_finnum_df, on=['ds', 'platform'],
        how='outer').merge(new_df_result,
                           on=['ds', 'platform'],
                           how='outer').merge(pay_df_result,
                                              on=['ds', 'platform'],
                                              how='outer').fillna(0))

    # 付费率、arpu、arppu
    result_df['spend_rate'] = result_df['pay_num'] * 1.0 / result_df['d1_num']
    result_df['arpu'] = result_df['income'] * 1.0 / result_df['d1_num']
    result_df['arppu'] = result_df['income'] * 1.0 / result_df['pay_num']
    result_df = result_df.fillna(0).rename(columns=rename_dic)
    result_df = result_df.replace({np.inf: 0})

    # 更新MySQL
    table = 'dis_daily_data'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    column = ['ds', 'platform', 'reg_user_num', 'dau', 'wau', 'mau', 'pay_num',
              'new_pay_num', 'income', 'spend_rate', 'arpu', 'arppu']
    update_mysql(table, result_df[column], del_sql)
    print date, table


if __name__ == '__main__':
    for platform in ['superhero2_tw']:
        settings_dev.set_env(platform)
        for date in date_range('20170909', '20171015'):
            print platform, date
            dis_daily_data(date)
