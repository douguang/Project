#!/usr/bin/env python
# -- coding: UTF-8 --
'''
@Time : 2017/7/24 0024 16:24
@Author : Zhang Yongchen
@File : tmp_20170724_dau_pay.py.py  根据开服日期，算出每个服务器自开服以来每日的dau和收入，孟伟需求。
@Software: PyCharm Community Edition
Description :
'''
from utils import hql_to_df
import pandas as pd
import settings_dev

def tmp_20170724_dau_pay(date):

    dau_sql = '''
        select user_id, reverse(substr(reverse(user_id), 8)) as server, ds from parse_info where ds>='20170629' and ds<='{date}'
    '''.format(date=date)
    print dau_sql
    dau_df = hql_to_df(dau_sql)

    pay_sql = '''
        select user_id, reverse(substr(reverse(user_id), 8)) as server, sum(order_money) pay, ds from raw_paylog where ds>='20170629' and ds<='{date}' and platform_2<>'admin_test' group by user_id, ds
    '''.format(date=date)
    print pay_sql
    pay_df = hql_to_df(pay_sql)

    server_sql = '''
        select reverse(substr(reverse(user_id), 8)) as server, min(ds) as server_date from parse_info where ds>='20170629' and ds<='{date}' group by server
    '''.format(date=date)
    print server_sql
    server_df = hql_to_df(server_sql)

    data = dau_df.merge(pay_df, on=['user_id', 'server', 'ds'], how='left').merge(server_df, on='server', how='left')
    data['day'] = (pd.to_datetime(data['ds']) - pd.to_datetime(data['server_date'])).dt.days + 1
    dau_data = pd.pivot_table(data=data, values='user_id', index=['server', 'server_date'], columns='day', aggfunc='count', fill_value=0)
    print dau_data.head(10)
    pay_data = pd.pivot_table(data=data, values='pay', index=['server', 'server_date'], columns='day', aggfunc='sum', fill_value=0)
    print pay_data.head(10)
    return dau_data, pay_data


if __name__ == '__main__':

    settings_dev.set_env('dancer_mul')
    dau_data, pay_data = tmp_20170724_dau_pay('20170723')
    dau_data.to_excel(r'E:\Data\output\dancer\dau_data.xlsx')
    pay_data.to_excel(r'E:\Data\output\dancer\pay_data.xlsx')