#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 用户 - 日常数据
Time        : 2017.05.03
illustration:
'''
import settings_dev
import pandas as pd
from utils import ds_add
from utils import update_mysql
from utils import hqls_to_dfs
from sqls_for_games.superhero import bi_act_sql
from sqls_for_games.superhero import act_sql
from sqls_for_games.superhero import gs_sql
from utils import date_range

act_days = [1, 7, 30]
name_dic = {'is_new_user': 'reg_user_num',
            'is_new_pay': 'new_pay_num',
            'order_money': 'income',
            'is_pay': 'pay_num'}
rename_dic = {'d1_num': 'dau', 'd7_num': 'wau', 'd30_num': 'mau'}


def dis_daily_data(date):
    act_dates = [ds_add(date, 1 - act_day) for act_day in act_days]
    act_dates_dic = {ds_add(date, 1 - act_day): 'd%d_num' % act_day
                     for act_day in act_days}
    assist_sql = '''
    SELECT ds,
           user_id,
           platform,
           plat,
           order_money,
           is_new_user,
           is_new_pay,
           case when order_money > 0 then 1 else 0 end as is_pay
    FROM mart_assist
    WHERE ds >= '{0}'
      AND ds <= '{1}'
    '''.format(act_dates[-1], date)
    info_sql = '''
    SELECT ds,
           uid AS user_id,
           substr(uid,1,1) AS plat,
           platform_2 AS platform
    FROM raw_info
    WHERE ds >= '{start_date}'
    AND ds <= '{end_date}'
    '''.format(start_date=act_dates[-1],
               end_date=date)
    if settings_dev.code in ['superhero_bi', 'superhero_self_en']:
        assist_df, info_df, gs_df, act_df = hqls_to_dfs(
            [assist_sql, info_sql, gs_sql,
             bi_act_sql.format(start_date=act_dates[-1],
                               end_date=date)])
        assist_df = pd.DataFrame(assist_df).drop_duplicates()
        act_df = act_df.merge(info_df,
                              on=['ds', 'user_id', 'plat', 'platform'],
                              how='outer').fillna('None')
    else:
        assist_df, info_df, gs_df, act_df = hqls_to_dfs(
            [assist_sql, info_sql, gs_sql,
             act_sql.format(start_date=act_dates[-1],
                            end_date=date)])
        act_df = act_df.merge(info_df,
                              on=['ds', 'user_id', 'plat'],
                              how='outer').fillna('None')
    assist_df = pd.DataFrame(assist_df).drop_duplicates()
    # 当日新用户、充值人数、收入、新增充值人数
    act_df = act_df.fillna(0)
    day_df = assist_df.loc[assist_df.ds == date]
    day_result = day_df.groupby(['ds', 'plat', 'platform']).agg({
        'is_new_user': 'sum',
        'order_money': 'sum',
        'is_new_pay': 'sum',
        'is_pay': 'sum',
    }).reset_index().rename(columns=name_dic)

    # 活跃用户数
    # 排除开服至今的gs数据
    act_df = act_df[~act_df['user_id'].isin(gs_df.user_id.values)]
    act_list = []
    for i in act_dates:
        # print i
        act_result = act_df.loc[(act_df.ds >= i) & (act_df.ds <= date)]
        act_result.loc[:, ['ds']] = i
        act_result = act_result.drop_duplicates(['user_id'])
        # print act_result
        act_list.append(act_result)
    act_num_df = pd.concat(act_list)
    act_num_df['act'] = 1
    act_finnum_df = (
        act_num_df.pivot_table('act', ['user_id', 'platform', 'plat'], 'ds')
        .reset_index().groupby(['platform', 'plat']).sum().reset_index()
        .rename(columns=act_dates_dic))
    act_finnum_df['ds'] = date
    result_df = day_result.merge(act_finnum_df,
                                 on=['ds', 'platform', 'plat'],
                                 how='outer').fillna(0)

    # 付费率、arpu、arppu
    result_df['spend_rate'] = result_df['pay_num'] * 1.0 / result_df['d1_num']
    result_df['arpu'] = result_df['income'] * 1.0 / result_df['d1_num']
    result_df['arppu'] = result_df['income'] * 1.0 / result_df['pay_num']
    result_df = result_df.fillna(0).rename(columns=rename_dic)

    # 更新MySQL
    table = 'dis_daily_data'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    column = ['ds', 'platform', 'reg_user_num', 'dau', 'wau', 'mau', 'pay_num',
              'new_pay_num', 'income', 'spend_rate', 'arpu', 'arppu']

    if settings_dev.code == 'superhero_bi':
        pub_result_df = result_df[result_df.plat == 'g'][column]
        ios_result_df = result_df[result_df.plat == 'a'][column]
        update_mysql(table, pub_result_df, del_sql, 'superhero_pub')
        update_mysql(table, ios_result_df, del_sql, 'superhero_ios')
    else:
        update_mysql(table, result_df[column], del_sql)

    print '{0} complete'.format(table)


if __name__ == '__main__':
    settings_dev.set_env('superhero_vt')
    for date in date_range('20170911', '20170911'):
        print date
        dis_daily_data(date)
