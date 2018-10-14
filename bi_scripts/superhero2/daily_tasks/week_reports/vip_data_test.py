#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author  : Dong Junshuang
Software: Sublime Text
Time    : 20170307
Description :  周报 - VIP数据
依赖的表: mid_info_all,raw_info,raw_paylog
'''
from utils import hql_to_df
from utils import ds_add
from utils import hqls_to_dfs
import pandas as pd
import settings_dev


def get_new_vip(date):
    '''本周新增VIP
    '''
    date_ago = ds_add(date, -7)
    print date_ago
    vip_sql = '''
    SELECT ds,
           vip as vip,
           count(user_id) vip_num
    FROM mid_info_all
    WHERE ds in ('{date}','{date_ago}')
      AND vip>0
    GROUP BY ds,
             vip
    ORDER BY vip
    '''.format(date=date, date_ago=date_ago)

    vip_df = hql_to_df(vip_sql)
    print vip_df
    columns_name = {
        '{date}'.format(date=date): 'now_num',
        '{date_ago}'.format(date_ago=date_ago): 'ago_num',
    }
    vip_data = (
        vip_df.pivot_table('vip_num', ['vip'],
                           'ds').reset_index().rename(columns=columns_name))
    # 本周新增VIP
    vip_data['new'] = vip_data['now_num'] - vip_data['ago_num']

    return vip_data[['vip', 'new']]


def get_pay_vip(date):
    '''本周 活跃VIP，活跃付费数，收入
    '''
    date_ago = ds_add(date, -6)
    info_sql = '''
    SELECT user_id,
           max(vip) vip
    FROM parse_info
    WHERE ds >='{date_ago}'
      AND ds <= '{date}'
      AND vip > 0
    GROUP BY user_id
    '''.format(date=date, date_ago=date_ago)
    pay_sql = '''
    SELECT user_id,
           sum(order_money) sum_rmb
    FROM raw_paylog
    WHERE ds >='{date_ago}'
      AND ds <= '{date}'
      AND platform <> 'admin_test'
    GROUP BY user_id
    '''.format(date=date, date_ago=date_ago)
    info_df, pay_df = hqls_to_dfs([info_sql, pay_sql])

    vip_df = info_df.merge(pay_df, on='user_id')
    # 本周VIP用户的活跃付费人数，收入
    pay_data = (vip_df.groupby('vip').agg({
        'user_id': 'count',
        'sum_rmb': 'sum',
    }).reset_index().rename(
        columns={'user_id': 'pay_num',
                 'sum_rmb': 'now_income'}))
    # 活跃VIP
    act_data = info_df.groupby('vip').agg({
        'user_id': 'count'
    }).reset_index().rename(
        columns={'user_id': 'act_num', })
    pay_result = act_data.merge(pay_data, on='vip', how='left')
    return pay_result


def get_result(date):
    new_vip_df = get_new_vip(date)
    print new_vip_df
    pay_vip_df = get_pay_vip(date)
    vip_df = pay_vip_df.merge(new_vip_df, on='vip', how='left')
    ranges = [0, 4, 9, 12, 14, 15]
    result = (
        vip_df.groupby(pd.cut(vip_df.vip, ranges).rename('vip_ivtl')).sum()
        .reset_index())
    result['arppu'] = result['now_income'] * 1.0 / result['pay_num']

    ivtl_result = result[['vip_ivtl', 'act_num', 'pay_num', 'new', 'arppu']]
    pay_result = pay_vip_df[['vip', 'now_income']]

    return ivtl_result, pay_result


if __name__ == '__main__':
    plat_list=['superhero2', ]
    for plat in plat_list:
        settings_dev.set_env(plat)
        # date = '20180124'
        for date in ['20180801']:
            print date
            ivtl_result, pay_result = get_result(date)
            ivtl_result.to_csv(
                r'C:\Users\Administrator\Desktop\0803\{plat}_vip_ivtl_result_{date}.csv'.format(date=date,plat=plat))
            pay_result.to_csv(
                r'C:\Users\Administrator\Desktop\0803\{plat}_vip_pay_result_{date}.csv'.format(date=date,plat=plat))
