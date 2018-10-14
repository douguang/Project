#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 充值的渠道收入统计
Time        : 2017.04.14
illustration:
'''
import settings_dev
from utils import hqls_to_dfs
from utils import update_mysql
from utils import date_range
from sqls_for_games.superhero import gs_sql


def dis_pay_platform(date):
    pay_sql = '''
    SELECT uid AS user_id,
           platform_2 AS platform,
           substr(uid,1,1) as plat,
           sum(order_money) AS order_money
    FROM raw_paylog
    WHERE ds = '{date}'
      AND platform_2 <> 'admin_test'
    GROUP BY uid,
             platform_2
    '''.format(date=date)
    pay_df, gs_df = hqls_to_dfs([pay_sql, gs_sql])
    # 排除开服至今的gs数据
    result_df = pay_df[~pay_df['user_id'].isin(gs_df.user_id.values)]
    result_df = result_df.groupby(
        ['platform', 'plat']).sum().order_money.reset_index()
    result_df['ds'] = date

    # 更新MySQL表
    table = 'dis_pay_platform'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    column = ['ds', 'platform', 'order_money']

    if settings_dev.code == 'superhero_bi':
        pub_result_df = result_df[result_df.plat == 'g'][column]
        ios_result_df = result_df[result_df.plat == 'a'][column]
        update_mysql(table, pub_result_df, del_sql, 'superhero_pub')
        update_mysql(table, ios_result_df, del_sql, 'superhero_ios')
    else:
        update_mysql(table, result_df[column], del_sql)

    print 'dis_pay_platform is complete'


if __name__ == '__main__':
    settings_dev.set_env('superhero_bi')
    for date in date_range('20170410', '20170416'):
        # date = '20170417'
        print date
        dis_pay_platform(date)
