#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 超级英雄-审计数据(pub、ios、qiku)
'''
import settings_dev
import pandas as pd
from utils import hqls_to_dfs, hql_to_df, ds_add
import datetime

def shenji_life_time(date):
    sql = '''
        SELECT t1.uid, firsttime, pay, freshtime
        FROM (SELECT uid, MIN(ds) AS firsttime, SUM(order_money) AS pay
            FROM raw_paylog
            WHERE ds >= '20160501'
                AND ds <= '{date}'
                AND platform_2 <> 'admin_test'
            GROUP BY uid
            ) t1
            LEFT JOIN (SELECT uid, max(ds) AS freshtime
                FROM raw_info
                WHERE ds >= '20160501'
                AND ds <= '{date}'
                GROUP BY uid
                ) t2 ON t1.uid = t2.uid
    '''.format(date=date)
    print sql
    df = hql_to_df(sql)
    # df.to_excel(
    #     r'E:\Data\output\superhero\df.xlsx')
    print df.head(10)
    df['days'] = (pd.to_datetime(df['freshtime']) - pd.to_datetime(df['firsttime'])).dt.days + 1
    print df.head(10)
    result_df = df.groupby('days').agg({'uid': lambda g: g.count(), 'pay': lambda g: g.sum()}).reset_index()
    # print result_df
    return result_df

if __name__ == '__main__':
    date = '20170930'
    settings_dev.set_env('superhero_bi')
    life_df = shenji_life_time(date)
    # life_df.to_excel(
    #     r'E:\Data\output\superhero\life_df_result_a.xlsx')
    settings_dev.set_env('superhero_qiku')
    q_life_df = shenji_life_time(date)
    # q_life_df.to_excel(
    #     r'E:\Data\output\superhero\life_df_result_q.xlsx')
    life_df_result = pd.concat(
        [life_df, q_life_df]).groupby('days').sum().reset_index()[['days', 'uid', 'pay']]
    print life_df_result
    life_df_result.to_excel(
        r'E:\Data\output\superhero\life_df_result.xlsx')