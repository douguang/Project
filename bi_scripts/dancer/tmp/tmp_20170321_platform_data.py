#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 武娘 新增注册、次留、三留、7留、14留、30留、充值金额、付费人数、DAU
create_date : 2016.07.17
'''
from utils import hql_to_df
import settings_dev
import pandas as pd
import numpy as np

def tmp_20170321_platform_data(start_date, date):

    #常规和充值数据
    info_sql = '''
        select t1.ds, t1.dau, t2.pay_num, t2.pay from
        (select count(user_id) as dau, ds from parse_info where ds>='{start_date}' and ds<='{date}' and account like '%meizu_%' group by ds) t1 left join
        (select count(distinct user_id) as pay_num, sum(order_money) as pay, ds from raw_paylog where ds>='{start_date}' and ds<='{date}' and platform_2 != 'admin_test' and order_id not like '%testktwwn%'
         and user_id in (select user_id from parse_info where ds>='{start_date}' and ds<='{date}' and account like '%meizu_%')group by ds) t2
        on t1.ds = t2.ds order by t1.ds
    '''.format(start_date=start_date, date=date)
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head(10)

    reg_sql = '''
        select count(distinct user_id) as reg_num, regexp_replace(to_date(reg_time), '-', '') as ds from mid_info_all where ds='{date}' and account like '%meizu_%'
        and regexp_replace(to_date(reg_time), '-', '')>='{start_date}' and regexp_replace(to_date(reg_time), '-', '')<='{date}' group by  regexp_replace(to_date(reg_time), '-', '')
    '''.format(date=date, start_date=start_date)
    reg_df = hql_to_df(reg_sql)
    print reg_df.head(10)
    info_df = info_df.merge(reg_df, on='ds', how='left')
    info_df.to_excel(r'E:\Data\output\dancer\meizu_info.xlsx')

    #留存数据
    liucun_sql = '''
        select account, min(regexp_replace(to_date(reg_time),'-','')) as regtime, ds from parse_info
        where ds>='{start_date}' and ds<='{date}' and regexp_replace(to_date(reg_time),'-','')>='{start_date}' and regexp_replace(to_date(reg_time),'-','')<='{date}'
        and account like '%meizu_%'
        group by ds, account
    '''.format(start_date=start_date, date=date)
    print liucun_sql
    liucun_df = hql_to_df(liucun_sql)
    print liucun_df.head(10)
    liucun_df['liucun'] = (pd.to_datetime(liucun_df['ds']) - pd.to_datetime(liucun_df['regtime'])).dt.days + 1
    print liucun_df.head(10)
    days = [1, 2, 3, 7, 14, 30]
    liucun_df = liucun_df[liucun_df['liucun'].isin(days)]
    liucun_df['num'] = 1
    liucun_result_df = pd.pivot_table(liucun_df, values='num', index='regtime', columns='liucun', aggfunc=np.sum, fill_value=0)
    print liucun_result_df.head(10)
    liucun_result_df.to_excel(r'E:\Data\output\dancer\meizu_liucun.xlsx')


if __name__ == '__main__':

    settings_dev.set_env('dancer_pub')
    result = tmp_20170321_platform_data('20161110', '20161210')
