#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 所有用户历史充值总额
Time        : 2017.04.07
illustration: history_money历史充值总额，包括当日充值总额
'''
import settings_dev
import pandas as pd
from utils import ds_add
from utils import hql_to_df
from utils import hqls_to_dfs
from sqls_for_games.superhero import gs_sql


def mart_paylog(date):
    # 当日的充值总额，充值钻石
    pay_sql = '''
    SELECT uid as user_id,
        sum(order_money) as order_money,
        sum(order_coin) as order_coin,
        sum(order_money) as history_money,
        sum(order_coin) as history_coin
    FROM raw_paylog
    WHERE ds = '{date}'
    GROUP BY uid
    '''.format(date=date)
    mid_info_sql = '''
    SELECT uid AS user_id,
           account
    FROM mid_info_all
    WHERE ds ='{date}'
    '''.format(date=date)
    pay_df, gs_df, mid_info_df = hqls_to_dfs([pay_sql, gs_sql, mid_info_sql])
    if pay_df.__len__() != 0 and mid_info_df.__len__() != 0:
        # 判断是否是开服日期
        if date == settings_dev.start_date.strftime('%Y%m%d'):
            result_df = pay_df.copy()
            # 是否是新增充值用户(UID)，是：1，否：0
            result_df['is_new_pay'] = 1
        else:
            history_pay_sql = '''
            SELECT user_id,
                history_money,
                history_coin
            FROM mart_paylog
            WHERE ds = '{yestoday}'
            '''.format(yestoday=ds_add(date, -1))
            history_pay_df = hql_to_df(history_pay_sql)
            # 是否是新增充值用户(UID)，是：1，否：0
            pay_df['is_new_pay'] = pay_df['user_id'].isin(
                history_pay_df.user_id.values)
            pay_df = pay_df.replace({'is_new_pay': {False: '1', True: '0'}})
            pay_df['is_new_pay'] = pay_df['is_new_pay'].astype('int')
            # 合并充值数据
            mart_result = pd.concat([pay_df, history_pay_df]).fillna(0)
            result_df = mart_result.groupby('user_id').sum().reset_index()

        # 排除测试用户
        result_df = result_df.merge(mid_info_df, on='user_id', how='left')
        result_df = result_df[~result_df['user_id'].isin(gs_df.user_id.values)]

        column = ['user_id', 'order_money', 'order_coin', 'history_money',
                  'history_coin', 'is_new_pay', 'account']
        result_df = result_df[column]
        print '--res--'
        print result_df.head(3)
        return result_df

# def mart_paylog(date):
#     # 补数据，适用于所有raw_paylog都有的版本
#     # 不判断是否是当日新增用户，且生成的当日的mart_paylog数据不要
#     pay_sql = '''
#     SELECT uid as user_id,
#         sum(order_money) as history_money,
#         sum(order_coin) as history_coin
#     FROM raw_paylog
#     WHERE ds <= '{date}'
#     GROUP BY uid
#     '''.format(date=date)
#     today_sql = '''
#     SELECT uid as user_id,
#         sum(order_money) as order_money,
#         sum(order_coin) as order_coin
#     FROM raw_paylog
#     WHERE ds = '{date}'
#     GROUP BY uid
#     '''.format(date=date)
#     pay_df, today_df, gs_df = hqls_to_dfs([pay_sql, today_sql, gs_sql])
#     result_df = pay_df.merge(today_df, on='user_id', how='outer').fillna(0)
#     # 不判断是否是当日新增用户，且当天数据不要
#     result_df['is_new_pay'] = 0
#     # 排除测试用户
#     result_df = result_df[~result_df['user_id'].isin(gs_df.user_id.values)]
#     column = ['user_id', 'order_money', 'order_coin',
#               'history_money', 'history_coin', 'is_new_pay']
#     result_df = result_df[column]

#     return result_df

# def mart_paylog(date):
#     # 补数据，适用于raw_paylog数据不全的版本,适用于超级英雄国内、越南版本
#     # 不判断是否是当日新增用户，且生成的当日的mart_paylog数据不要
#     pay_ago_sql = '''
#     SELECT uid as user_id,
#         sum(order_money) as history_money,
#         sum(order_coin) as history_coin
#     FROM total_paylog
#     WHERE regexp_replace(substr(order_time,1,10),'-','') <= '20170221'
#     GROUP BY uid
#     '''.format(date=date)
#     pay_sql = '''
#     SELECT uid as user_id,
#         sum(order_money) as history_money,
#         sum(order_coin) as history_coin
#     FROM raw_paylog
#     WHERE ds > '20170221'
#     and ds <= '{date}'
#     GROUP BY uid
#     '''.format(date=date)
#     today_sql = '''
#     SELECT uid as user_id,
#         sum(order_money) as order_money,
#         sum(order_coin) as order_coin
#     FROM raw_paylog
#     WHERE ds = '{date}'
#     GROUP BY uid
#     '''.format(date=date)
#     # pay_ago_df, today_df, gs_df = hqls_to_dfs(
#     #     [pay_ago_sql, today_sql, gs_sql])
#     pay_ago_df, pay_df, today_df, gs_df = hqls_to_dfs(
#         [pay_ago_sql, pay_sql, today_sql, gs_sql])
#     pay_result = pd.concat([pay_ago_df, pay_df])
#     pay_result = pay_result.groupby('user_id').sum().reset_index()
#     # pay_result = pay_ago_df
#     result_df = pay_result.merge(today_df, on='user_id', how='outer').fillna(0)
#     # 不判断是否是当日新增用户，且当天数据不要
#     result_df['is_new_pay'] = 0
#     # 排除测试用户
#     result_df = result_df[~result_df['user_id'].isin(gs_df.user_id.values)]
#     column = ['user_id', 'order_money', 'order_coin',
#               'history_money', 'history_coin', 'is_new_pay']
#     result_df = result_df[column]

#     return result_df

#     '''
#     测试sql
#     ------------
#     select count(distinct user_id) from mart_paylog where ds ='20170418'
#     ------------
#     select count(distinct uid) from (
#     select distinct uid from total_paylog where regexp_replace(substr(order_time,1,10),'-','') <= '20170221'
#     union all
#     select distinct uid from raw_paylog where ds> '20170221' and ds <='20170418'
#     )a
#     ------------
#     select count(distinct uid) from mid_gs
#     '''

if __name__ == '__main__':
    settings_dev.set_env('superhero_vt')
    date = '20170708'
    result_df = mart_paylog(date)

