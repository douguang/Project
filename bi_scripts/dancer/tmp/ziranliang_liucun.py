#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 卡牌飞升（转生），字段evo,0-15转
Name        : liucun_dancer
Original    : liucun_dancer
'''
import settings_dev
import pandas as pd
from utils import hql_to_df, update_mysql, get_config, date_range

def liucun_dancer(start_date, date):

    liucun_sql = '''
        select account, min(regexp_replace(to_date(reg_time),'-','')) as regtime, ds, max(vip), max(level) from parse_info where ds>='{start_date}' and ds<='{date}' and regexp_replace(to_date(reg_time),'-','')>='{start_date}' group by ds, account
    '''.format(date=date, start_date=start_date)
    print liucun_sql
    liucun_df = hql_to_df(liucun_sql)
    print liucun_df.head(10)
    ziran_df = pd.read_excel(r'E:\Data\output\dancer\account_0307.xlsx')
    print ziran_df.head(10)
    liucun_df = liucun_df[liucun_df['account'].isin(ziran_df['account'])]
    # liucun_df['regtime'] = liucun_df['regtime'].astype(int)
    # liucun_df['ds'] = liucun_df['ds'].astype(int)
    # liucun_df['liucun'] = liucun_df['ds'] - liucun_df['regtime'] + 1
    print liucun_df.head(25)
    return liucun_df

if __name__ == '__main__':
    settings_dev.set_env('dancer_pub')
    # for date in date_range('20161110', '20161121'):
    #         liucun_dancer(date)
    result = liucun_dancer('20170307', '20170320')
    result.to_excel(r'E:\Data\output\dancer\ziranliang_liucun.xlsx')