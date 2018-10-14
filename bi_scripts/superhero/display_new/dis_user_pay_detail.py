#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 营收 - 用户付费情况
Time        : 2017.05.03
illustration:
'''
import settings_dev
import numpy as np
from utils import ds_add
from utils import hqls_to_dfs
from utils import update_mysql


def dis_user_pay_detail(date):
    reg_sql = '''
    SELECT ds,
           plat,
           account
    FROM mid_new_account
    WHERE ds = '{date}'
    GROUP BY ds,
             plat,
             account
    '''.format(date=date)
    assist_sql = '''
    SELECT account,
           plat,
           sum(order_money) AS income
    FROM mart_assist
    WHERE ds = '{date}'
    GROUP BY account,
             plat
    '''.format(date=date)
    pay_sql = '''
    SELECT account
    FROM mart_paylog
    WHERE ds = '{yestoday}'
    GROUP BY account
    '''.format(yestoday=ds_add(date, -1))
    reg_df, assist_df, pay_df = hqls_to_dfs([reg_sql, assist_sql, pay_sql])
    # 当日是否充值
    assist_df['dau_pay'] = assist_df['income'].map(lambda s: 1 if s > 0 else 0)
    # 新用户数
    reg_result = (reg_df.groupby(['ds', 'plat']).count().reset_index().rename(
        columns={'account': 'reg_user_num'}))
    # dau数据
    dau_dic = {'account': 'dau', 'dau_pay': 'pay_num'}
    dau_result = assist_df.groupby('plat').agg({
        'account': 'count',
        'dau_pay': 'sum',
        'income': 'sum',
    }).reset_index().rename(columns=dau_dic)
    # 新增数据
    new_df = assist_df[(~assist_df['account'].isin(pay_df.account.values)) & (
        assist_df.dau_pay == 1)]
    new_dic = {'dau_pay': 'new_pay_num', 'income': 'new_income'}
    new_result = new_df.groupby('plat').agg({
        'dau_pay': 'sum',
        'income': 'sum',
    }).reset_index().rename(columns=new_dic)
    # 当日新增付费数据
    day_new_df = reg_df.loc[reg_df.ds == date].merge(assist_df,
                                                     on=['account', 'plat'],
                                                     how='left')
    day_new_dic = {'dau_pay': 'new_day_pay_num', 'income': 'new_day_income'}
    day_new_result = day_new_df.groupby('plat').agg({
        'dau_pay': 'sum',
        'income': 'sum',
    }).reset_index().rename(columns=day_new_dic)
    # 整合数据
    result_df = (
        reg_result.merge(dau_result, on='plat',
                         how='outer')
        .merge(new_result, on='plat', how='outer').merge(day_new_result))
    result_df['pay_rate'] = result_df['pay_num'] * 1.0 / result_df['dau']
    result_df['arppu'] = result_df['income'] * 1.0 / result_df['pay_num']
    result_df['arpu'] = result_df['income'] * 1.0 / result_df['dau']
    result_df['new_pay_rate'] = result_df['new_pay_num'] * \
        1.0 / result_df['dau']
    result_df['new_arppu'] = result_df['new_income'] * \
        1.0 / result_df['new_pay_num']
    result_df['new_arpu'] = result_df['new_income'] * 1.0 / result_df['dau']
    result_df['new_day_pay_rate'] = result_df['new_day_pay_num'] * \
        1.0 / result_df['reg_user_num']
    result_df['new_day_arppu'] = result_df['new_day_income'] * \
        1.0 / result_df['new_day_pay_num']
    result_df['new_day_arpu'] = result_df['new_day_income'] * \
        1.0 / result_df['reg_user_num']
    # 老玩家付费数据
    result_df['old_dau'] = result_df['dau'] - result_df['reg_user_num']
    result_df['old_pay_num'] = result_df['pay_num'] - \
        result_df['new_day_pay_num']
    result_df['old_income'] = result_df['income'] * \
        1.0 / result_df['new_day_income']
    result_df['old_pay_rate'] = result_df['old_pay_num'] * \
        1.0 / result_df['old_dau']
    result_df['old_arppu'] = result_df['old_income'] * \
        1.0 / result_df['old_pay_num']
    result_df['old_arpu'] = result_df['old_income'] * \
        1.0 / result_df['old_dau']
    result_df = result_df.fillna(0)
    result_df = result_df.replace({np.inf: 0})

    print result_df

    # 更新MySQL
    table = 'dis_user_pay_detail'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    column_list = ['pay_num', 'income', 'pay_rate', 'arppu', 'arpu']
    column = ['ds', 'reg_user_num', 'dau'] + column_list + [
        'new_%s' % i for i in column_list
    ] + ['new_day_%s' % i for i in column_list
         ] + ['old_dau'] + ['old_%s' % i for i in column_list]

    if settings_dev.code == 'superhero_bi':
        pub_result_df = result_df[result_df.plat == 'g'][column]
        ios_result_df = result_df[result_df.plat == 'a'][column]
        update_mysql(table, pub_result_df, del_sql, 'superhero_pub')
        update_mysql(table, ios_result_df, del_sql, 'superhero_ios')
    else:
        update_mysql(table, result_df[column], del_sql)

    print '{0} complete'.format(table)


if __name__ == '__main__':
    settings_dev.set_env('superhero_bi')
    date = '20170507'
    dis_user_pay_detail(date)
