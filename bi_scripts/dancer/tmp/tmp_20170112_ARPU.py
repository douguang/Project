#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 武娘台服 新服14天ARPU，秦祺书需求。
Database    : dancer_tw
'''
import settings_dev
import pandas as pd
from utils import hql_to_df, ds_add, date_range
from ipip import *

def tmp_20170112_new_server_ARPU(date):

    #开服日期
    begin_sql = '''
        select reverse(substr(reverse(user_id), 8)) as server, min(ds) as begin_time from parse_info where ds>='{date}' group by server
    '''.format(date=date)
    print begin_sql
    begin_df = hql_to_df(begin_sql)
    print begin_df.head(10)

    #DAU
    dau_sql = '''
        select reverse(substr(reverse(user_id), 8)) as server, count(user_id) as dau, ds from parse_info where ds>='{date}' group by server, ds
    '''.format(date=date)
    print dau_sql
    dau_df = hql_to_df(dau_sql)
    print dau_df.head(10)

    #pay
    pay_sql = '''
        select reverse(substr(reverse(user_id), 8)) as server, sum(order_money) as pay, ds from raw_paylog where ds>='{date}' group by server, ds
    '''.format(date=date)
    print pay_sql
    pay_df = hql_to_df(pay_sql)
    print pay_df.head(10)

    #spend
    spend_sql = '''
        select reverse(substr(reverse(user_id), 8)) as server, sum(coin_num) as spend, ds from raw_spendlog where ds>='{date}' group by server, ds
    '''.format(date=date)
    print spend_sql
    spend_df = hql_to_df(spend_sql)
    print spend_df.head(10)

    result_df = dau_df.merge(begin_df, on='server', how='left').merge(pay_df, on=['server', 'ds'], how='left').merge(spend_df, on=['server', 'ds'], how='left')
    print result_df.head(10)

    result_df['rank'] = result_df['ds'].groupby(result_df['server']).rank()
    result_df = result_df[result_df['rank'] <= 14]
    result_df = result_df.sort(['server', 'ds']).reset_index()
    print result_df.head(10)

    return result_df


if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    result = tmp_20170112_new_server_ARPU('20160907')
    result.to_excel(r'E:\Data\output\dancer\dancer_tw_ARPU.xlsx')
