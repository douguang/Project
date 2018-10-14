#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 日常数据
Time        : 2017.03.17
'''
import settings_dev
from utils import ds_add, date_range
from utils import hql_to_df, hqls_to_dfs
import pandas as pd
from utils import update_mysql
# from utils import hqls_to_dfs
# from utils import date_range

act_days = [1, 7, 30]


def dis_daily_data(date):

    act_dates = [ds_add(date, 1 - act_day) for act_day in act_days]
    act_dates_dic = {ds_add(date, 1 - act_day): 'd%d_num' % act_day
                     for act_day in act_days}
    rename_dic = {'d1_num': 'dau', 'd7_num': 'wau', 'd30_num': 'mau'}

    act_sql = '''
    SELECT ds,
           user_id,
           platform
    FROM raw_info
    WHERE ds >= '{0}'
      AND ds <= '{1}'
      AND  user_id <> 'None'
    '''.format(act_dates[-1], date)
    act_df = hql_to_df(act_sql)

    # 活跃用户数
    act_list = []
    for i in act_dates:
        act_result = act_df.loc[(act_df.ds >= i) & (act_df.ds <= date)]
        act_result.loc[:, ['ds']] = i
        act_result = act_result.drop_duplicates(['user_id'])
        act_list.append(act_result)
    act_num_df = pd.concat(act_list)
    act_num_df['act'] = 1
    act_finnum_df = (
        act_num_df.pivot_table('act', ['user_id', 'platform'], 'ds')
        .reset_index().groupby('platform').sum().reset_index()
        .rename(columns=act_dates_dic).rename(columns=rename_dic))
    print act_finnum_df
    # 注册用户数
    reg_sql = '''
    SELECT ds,
           platform,
           count(user_id) reg_user_num
    FROM raw_info
    WHERE ds = '{date}'
      AND regexp_replace(substr(reg_time,1,10),'-','') = '{date}'
      AND  user_id <> 'None'
    GROUP BY ds,
             platform
    '''.format(date=date)
    reg_df = hql_to_df(reg_sql)

    # 新增充值用户
    new_order_sql = '''
    select count(t3.user_id) as new_pay_num, t3.platform from
(select t1.user_id, t2.platform from (
  SELECT user_id FROM raw_paylog WHERE ds <= '{date}' AND admin = 0 and status = 1 GROUP BY user_id  HAVING min(ds) = '{date}') t1
    left join
    (select user_id, platform from raw_info where ds='{date}') t2
    on t1.user_id = t2.user_id
  ) t3 group by platform
    '''.format(date=date)

    # 当日充值数据
    pay_sql = '''
    select count(t3.user_id) as pay_num, sum(t3.pay_money) as income, t3.platform from
(select t1.user_id, t1.pay_money, t2.platform from (
  SELECT user_id, sum(order_rmb) AS pay_money FROM raw_paylog WHERE ds = '{date}' AND admin = 0 and status = 1 GROUP BY user_id) t1
    left join
    (select user_id, platform from raw_info where ds='{date}') t2
    on t1.user_id = t2.user_id
  ) t3 group by platform
    '''.format(date=date)
    # print pay_sql

    new_order_df, pay_df = hqls_to_dfs([new_order_sql, pay_sql])
    print reg_df, pay_df

    result_df = reg_df.merge(act_finnum_df, on='platform', how='outer').merge(new_order_df, on='platform', how='outer').merge(pay_df, on='platform', how='outer')
    result_df['pay_rate'] = result_df['pay_num'] * 1.0 / result_df['dau']
    result_df['arpu'] = result_df['income'] * 1.0 / result_df['dau']
    result_df['arppu'] = result_df['income'] * 1.0 / result_df['pay_num']
    result_df['ds'] = date
    columns = ['ds', 'platform', 'reg_user_num', 'dau', 'wau', 'mau', 'pay_num', 'new_pay_num', 'income', 'pay_rate', 'arpu', 'arppu']
    result_df = result_df[columns].fillna(0)
    print result_df

    # 更新MySQL表
    table = 'dis_daily_data'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)



if __name__ == '__main__':
    settings_dev.set_env('jianniang_tw')
    for date in date_range('20170706', '20170828'):
        dis_daily_data(date)
