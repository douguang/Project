#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-4-6 下午3:59
@Author  : Andy 
@File    : shenji3.py
@Software: PyCharm
Description :
'''

import settings_dev
from utils import hql_to_df, ds_add
import pandas as pd
import time, datetime


def tmp_20170405_week_data(date):

    #充值
    pay_sql = '''
        select '{date}' as monday, count(distinct user_id) as pay_num, sum(order_coin) as pay_coin from raw_paylog where ds>='{date}' and ds<='{date_7}' and platform_2 != 'admin_test' and order_id not like '%testktwwn%'
    '''.format(date=date, date_7=ds_add(date, 6))
    print pay_sql
    pay_df = hql_to_df(pay_sql)
    print pay_df.head(10)

    #消费
    spend_sql = '''
        select '{date}' as monday, sum(coin_num) as spend_coin from raw_spendlog where ds>='{date}' and ds<='{date_7}' and user_id in (
        select distinct user_id from raw_paylog where ds>='{date}' and ds<='{date_7}' and platform_2 != 'admin_test' and order_id not like '%testktwwn%')
    '''.format(date=date, date_7=ds_add(date, 6))
    print spend_sql
    spend_df = hql_to_df(spend_sql)
    #下周消费
    spend_next_sql = '''
        select '{date}' as monday, sum(coin_num) as spend_next from raw_spendlog where ds>='{date_8}' and ds<='{date_14}' and user_id in (
        select distinct user_id from raw_paylog where ds>='{date}' and ds<='{date_7}' and platform_2 != 'admin_test' and order_id not like '%testktwwn%')
    '''.format(date=date, date_7=ds_add(date, 6), date_8=ds_add(date, 7), date_14=ds_add(date, 13))
    print spend_next_sql
    spend_next_df = hql_to_df(spend_next_sql)

    #合并
    all_df = pay_df.merge(spend_df, on='monday', how='left').merge(spend_next_df, on='monday', how='left')
    print all_df.head(10)
    return all_df

if __name__ == '__main__':

    settings_dev.set_env('sanguo_ks')
    result_list = []
    for i in range(15):
        date = '20160926'
        n = i * 7
        date = ds_add(date, n)
        result_list.append(tmp_20170405_week_data(date))
    result = pd.concat(result_list)
    result.to_excel(r'/home/kaiqigu/桌面/机甲无双-金山-审计-10月11月12月2.xlsx', index=False)