#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description :  武娘国服 1月1日至今  每日新增和 3 、7、14日留存 （account），韩鹏需求。
Database    : dancer_pub
'''
import settings_dev
import pandas as pd
from utils import hql_to_df, ds_add, date_range, hqls_to_dfs
from ipip import *

def tmp_20170112_country_loss(date_start, date):

    # 注册数据
    reg_sql = '''
       select min(regexp_replace(to_date(reg_time),'-','')) as regtime, account, substr(account, 1, instr(account, '_')-1) as platform
       from mid_info_all
       where ds = '{date}' and
         regexp_replace(to_date(reg_time),'-','') >= '{date_start}' and regexp_replace(to_date(reg_time),'-','') <= '{date}'
       group by account
       '''.format(date=date, date_start=date_start)
    print reg_sql

    # 登录
    load_sql = '''
        select account, ds from parse_info where ds >= '{date_start}' and ds <= '{date}' group by account, ds
    '''.format(date=date, date_start=date_start)
    print load_sql

    reg_df = hql_to_df(reg_sql)
    load_df = hql_to_df(load_sql)
    data = reg_df.merge(load_df, on='account', how='right')
    data['days'] = (pd.to_datetime(data['regtime']) - pd.to_datetime(data['ds'])).dt.days+1
    result_df = pd.pivot_table(data, index=['platform', 'regtime'], columns='days', values='account', fill_value=0, aggfunc='count').reset_index()
    print result_df
    return result_df

if __name__ == '__main__':
    settings_dev.set_env('dancer_pub')
    result = tmp_20170112_country_loss('20161104', '20170101')
    result.to_excel(r'E:\Data\output\dancer\platform_loss.xlsx')