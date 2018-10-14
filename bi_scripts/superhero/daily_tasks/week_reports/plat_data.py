#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Author  : Dong Junshuang
@Software: Sublime Text
@Time    : 20170307
Description :  周报 - 渠道数据
'''
from utils import hql_to_df
from utils import hqls_to_dfs
from utils import ds_add
from utils import get_rank
import pandas as pd
import settings_dev


def get_df(week_ago, date):
    info_sql = '''
    SELECT  ds,
            uid,
            CASE
                WHEN platform_2 IN ('37wan','37wanA668','37wanA669') THEN '37wan'
                WHEN platform_2 IN ('androidcmge','cmgeapp','cmge') THEN 'cmge'
                WHEN platform_2 IN ('haima','androidhaima') THEN 'haima'
                ELSE platform_2
            END AS platform_2,
            regexp_replace(substr(create_time,1,10),'-','') reg_time
    FROM raw_info
    WHERE ds >='{week_ago}'
      AND ds <= '{date}'
    '''.format(week_ago=week_ago, date=date)
    pay_sql = '''
    SELECT ds,
           uid,
           order_money as order_rmb
    FROM raw_paylog
    WHERE ds >='{week_ago}'
      AND ds <= '{date}'
      AND platform_2 <> 'admin_test'
    '''.format(week_ago=week_ago, date=date)
    info_df, pay_df = hqls_to_dfs([info_sql, pay_sql])
    return info_df, pay_df


def get_act_df(now_df, last_df):
    #从新构造df,字段为act_num、platform_2
    act_df = (now_df.groupby('platform_2').agg({
        'uid': 'nunique'
    }).reset_index().rename(columns={'uid': 'act_num'}))

    last_act_df = (last_df.groupby('platform_2').agg({
        'uid': 'nunique'
    }).reset_index().rename(columns={'uid': 'last_act_num'}))
    #act_df外连接last_act_df，主键platform_2字段
    act_result = act_df.merge(last_act_df, on='platform_2', how='outer')
    #返回一个df
    df = get_rank(act_result, 'act_num', 9)[['platform_2', 'last_act_num', 'act_num', 'rank']]

    other_df = (pd.DataFrame(
        {'platform_2': ['other'],
         'act_num':     [act_df.sum().act_num - df.sum().act_num],
         'last_act_num':[last_act_df.sum().last_act_num - df.sum().last_act_num],
         'rank': [10]
         })[['platform_2', 'last_act_num', 'act_num', 'rank']])

    result = pd.concat([df, other_df])
    return result


def get_rank_ago(df, column):
    df = df.sort_values(by=column, ascending=False)
    df['rank_ago'] = range(1, (len(df) + 1))
    return df


def get_pay_df(now_pay_df, last_pay_df):
    now_income_df = (now_pay_df.groupby('platform_2').agg({
        'order_rmb': 'sum'
    }).reset_index().rename(columns={'order_rmb': 'sum_rmb'}))

    last_income_df = (last_pay_df.groupby('platform_2').agg({
        'order_rmb': 'sum'
    }).reset_index().rename(columns={'order_rmb': 'last_sum_rmb'}))

    last_income_df = get_rank_ago(last_income_df, 'last_sum_rmb')

    income_result = now_income_df.merge(
        last_income_df, on='platform_2', how='outer')
    df = get_rank(income_result, 'sum_rmb', 9)
    other_df = (pd.DataFrame(
        {'platform_2': ['other'],
         'sum_rmb': [now_income_df.sum().sum_rmb - df.sum().sum_rmb],
         'last_sum_rmb':
         [last_income_df.sum().last_sum_rmb - df.sum().last_sum_rmb],
         'rank': [10]})[['platform_2', 'sum_rmb', 'last_sum_rmb', 'rank']])
    result = pd.concat([df, other_df])
    result['change'] = result['rank_ago'] - result['rank']
    result['huan'] = (result['sum_rmb'] -
                      result['last_sum_rmb']) / result['last_sum_rmb']
    columns = ['platform_2', 'rank', 'sum_rmb',
               'last_sum_rmb', 'huan', 'rank_ago', 'change']
    result = result[columns]
    return result


if __name__ == '__main__':
    plat = 'superhero_bi'
    settings_dev.set_env(plat)
    # date = '20170301'
    for date in ['20180307']:
        print date
        date_ago = ds_add(date, -6)
        week_ago = ds_add(date, -13)
        info_df, pay_df = get_df(week_ago, date)

        # 本周活跃，上周活跃
        now_df = info_df[(info_df.ds >= date_ago) & (info_df.ds <= date)]
        last_df = info_df[(info_df.ds >= week_ago) & (info_df.ds < date_ago)]
        act_result = get_act_df(now_df, last_df)

        # 本周新增，上周新增
        now_reg_df = now_df[(now_df.reg_time >= date_ago) & (now_df.reg_time <= date)]
        last_reg_df = last_df[(last_df.reg_time >= week_ago) & (last_df.reg_time <
                                                                date_ago)]
        reg_result = get_act_df(now_reg_df, last_reg_df)

        # 本周收入，上周收入
        pay_result = info_df.merge(pay_df, on=['ds', 'uid'])
        now_pay_df = pay_result[(pay_result.ds >= date_ago) & (pay_result.ds <= date)]
        last_pay_df = pay_result[(pay_result.ds >= week_ago) & (pay_result.ds < date_ago)]

        income_result = get_pay_df(now_pay_df, last_pay_df)
        income_result = income_result.T

        act_result.to_csv(
            r'/Users/kaiqigu/Documents/Superhero/超英周报数据/{plat}_plat_act_result_{date}.csv'.format(date=date,plat=plat))
        reg_result.to_csv(
            r'/Users/kaiqigu/Documents/Superhero/超英周报数据/{plat}_plat_reg_result_{date}.csv'.format(date=date,plat=plat))
        income_result.to_csv(
            r'/Users/kaiqigu/Documents/Superhero/超英周报数据/{plat}_plat_income_result_{date}.csv'.format(date=date,plat=plat))
