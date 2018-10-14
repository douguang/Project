#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 武娘多语言 - 日常数据
create_date : 2016.07.17
Illustration:
'''
import settings_dev
import pandas as pd
import numpy as np
from utils import ds_add
from utils import hqls_to_dfs
from utils import update_mysql
from dancer.cfg import LAN_TYPE
from utils import date_range

act_days = [1, 7, 30]
rename_dic = {'d1_num': 'dau', 'd7_num': 'wau', 'd30_num': 'mau'}


def get_div(df, res_col, div_chu, div_beichu):
    if div_beichu == 0:
        return 0
    else:
        df[res_col] = df[div_chu] * 1.0 / df[div_beichu]
        return df


def dis_daily_data_mul(date):
    act_dates = [ds_add(date, 1 - act_day) for act_day in act_days]
    act_dates_dic = {ds_add(date, 1 - act_day): 'd%d_num' % act_day
                     for act_day in act_days}
    # 新增充值用户 - 分服
    new_order_sql = '''
    SELECT user_id
    FROM raw_paylog
    WHERE ds <= '{date}'
      AND platform_2 <> 'admin_test'
      AND order_id NOT LIKE '%test%'
    GROUP BY user_id HAVING min(ds) = '{date}'
    '''.format(date=date)
    # 当日充值数据
    pay_sql = '''
    SELECT user_id,
           sum(order_money) AS pay_money
    FROM raw_paylog
    WHERE ds = '{date}'
      AND platform_2 <> 'admin_test'
      AND order_id NOT LIKE '%test%'
    GROUP BY user_id
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
                    user_id,
                    register_lan_sort as language
    FROM parse_info
    WHERE ds >= '{start_date}'
      AND ds <= '{end_date}'
    '''.format(start_date=act_dates[-1],
               end_date=date)

    new_order_df, new_df, pay_df, info_df = hqls_to_dfs(
        [new_order_sql, new_sql, pay_sql, info_sql])

    # 活跃用户数
    day_info_df = info_df[info_df.ds == date]
    # 匹配语言
    new_order_df = new_order_df.merge(day_info_df,
                                      on='user_id',
                                      how='left').fillna('None')
    new_df = new_df.merge(day_info_df,
                          on=['ds', 'user_id'],
                          how='left').fillna('None')
    pay_df = pay_df.merge(day_info_df, on='user_id', how='left').fillna('None')

    # 汇总数据
    # 新增充值用户
    new_order_result = new_order_df.groupby(
        ['ds', 'language']).count().reset_index().rename(
            columns={'user_id': 'new_pay_num'})
    # 新注册用户
    new_df_result = new_df.groupby(
        ['ds', 'language']).count().reset_index().rename(
            columns={'user_id': 'reg_user_num'})
    # 充值总额
    pay_df_result = pay_df.groupby(['ds', 'language']).agg({
        'user_id': 'count',
        'pay_money': 'sum'
    }).reset_index().rename(columns={'user_id': 'pay_num',
                                     'pay_money': 'income'})
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
        act_num_df.pivot_table('act', ['user_id', 'language'], 'ds')
        .reset_index().groupby('language').sum().reset_index()
        .rename(columns=act_dates_dic))
    act_finnum_df['ds'] = date

    # 最终结果
    result_df = (new_order_result.merge(
        new_df_result, on=['ds', 'language'],
        how='outer').merge(act_finnum_df,
                           on=['ds', 'language'],
                           how='outer').merge(pay_df_result,
                                              on=['ds', 'language'],
                                              how='outer'))

    # 付费率、arpu、arppu
    result_df = get_div(result_df, 'spend_rate', 'pay_num', 'd1_num')
    result_df = get_div(result_df, 'arpu', 'income', 'd1_num')
    result_df = get_div(result_df, 'arppu', 'income', 'pay_num')
    result_df = result_df.fillna(0).rename(columns=rename_dic)
    result_df = result_df.replace({'language': LAN_TYPE})
    result_df = result_df.replace({np.inf: 0})

    # 更新MySQL
    table = 'dis_daily_data'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    column = ['ds', 'language', 'reg_user_num', 'dau', 'wau', 'mau', 'pay_num',
              'new_pay_num', 'income', 'spend_rate', 'arpu', 'arppu']
    update_mysql(table, result_df[column], del_sql)
    print date, table


if __name__ == '__main__':
    settings_dev.set_env('dancer_mul')
    for date in date_range('20170707', '20170707'):
        print date
        dis_daily_data_mul(date)
