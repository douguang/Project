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
from utils import hql_to_df
from utils import date_range

act_days = [1, 7, 30]
name_dic = {'is_new_user': 'reg_user_num',
            'is_new_pay': 'new_pay_num', 'order_money': 'income', 'is_pay': 'pay_num'}
rename_dic = {'d1_num': 'dau', 'd7_num': 'wau', 'd30_num': 'mau'}

if __name__ == '__main__':
    settings_dev.set_env('superhero_bi')
    date = '20170502'

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
    assist_df = hql_to_df(assist_sql)
    assist_df = assist_df.drop_duplicates()
    # 当日新用户、充值人数、收入、新增充值人数
    day_df = assist_df.loc[assist_df.ds == date]
    day_result = day_df.groupby(['ds', 'plat', 'platform']).agg({
        'is_new_user': 'sum',
        'order_money': 'sum',
        'is_new_pay': 'sum',
        'is_pay': 'sum',
    }).reset_index().rename(columns=name_dic)

    # 活跃用户数
    act_list = []
    for i in act_dates:
        # print i
        act_result = assist_df.loc[(assist_df.ds >= i)
                                   & (assist_df.ds <= date)]
        act_result.loc[:, ['ds']] = i
        act_result = act_result.drop_duplicates(['user_id'])
        # print act_result
        act_list.append(act_result)
    act_num_df = pd.concat(act_list)
    act_num_df['act'] = 1
    act_finnum_df = (act_num_df.pivot_table('act', ['user_id', 'platform', 'plat'], 'ds')
                     .reset_index().groupby(['platform', 'plat']).sum().reset_index()
                     .rename(columns=act_dates_dic))
    act_finnum_df['ds'] = date
    result_df = day_result.merge(
        act_finnum_df, on=['ds', 'platform', 'plat'], how='outer').fillna(0)

    # 付费率、arpu、arppu
    result_df['spend_rate'] = result_df['pay_num'] * 1.0 / result_df['d1_num']
    result_df['arpu'] = result_df['income'] * 1.0 / result_df['d1_num']
    result_df['arppu'] = result_df['income'] * 1.0 / result_df['pay_num']
    result_df = result_df.fillna(0).rename(columns=rename_dic)

    # 更新MySQL
    table = 'dis_daily_data_new'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    column = ['ds', 'platform', 'reg_user_num', 'dau', 'wau', 'mau',
              'pay_num', 'new_pay_num', 'income', 'spend_rate', 'arpu',
              'arppu']

    if settings_dev.code == 'superhero_bi':
        pub_result_df = result_df[result_df.plat == 'g'][column]
        ios_result_df = result_df[result_df.plat == 'a'][column]
        update_mysql(table, pub_result_df, del_sql, 'superhero_pub')
        update_mysql(table, ios_result_df, del_sql, 'superhero_ios')
    else:
        update_mysql(table, result_df[column], del_sql)

    print '{0} complete'.format(table)
