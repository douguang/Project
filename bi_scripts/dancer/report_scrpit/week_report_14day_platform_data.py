#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 周报 2周的渠道数据
'''

from utils import hql_to_df, ds_add, date_range
import settings_dev
import time
import datetime
import pandas as pd


print '''周四执行'''


def formatDate(Date, formatType='YYYY-MM-DD'):
    formatType = formatType.replace('YYYY', Date[0:4])
    formatType = formatType.replace('MM', Date[4:6])
    formatType = formatType.replace('DD', Date[-2:])
    return formatType


def weekly_data(date):
    #武娘
    sql = '''
    select t1.user_id, t1.ds, t1.regtime, t3.platform, t2.pay from (
        select user_id, ds, regexp_replace(substr(reg_time,1,10),'-','') as regtime from parse_info where ds>='{date_7}' and ds<='{date}') t1
        left join (
        select user_id, ds, sum(order_money) as pay from raw_paylog where ds>='{date_7}' and ds<='{date}' and platform_2<>'admin_test' and order_id not like '%testktwwn%' group by user_id, ds) t2
        on (t1.ds = t2.ds and t1.user_id= t2.user_id) left join (
        select user_id, ds, platform from parse_actionlog where ds>='{date_7}' and ds<='{date}' group by user_id, ds, platform) t3
        on (t1.ds = t3.ds and t1.user_id= t3.user_id)
    '''.format(date=date, date_7=ds_add(date, -6))
    print sql
    df = hql_to_df(sql).fillna(0)
    # print df.head(10)
    dau_df = df.groupby('platform').user_id.nunique().reset_index().rename(columns={'user_id': 'wau'})
    # print dau_df.head(10)
    dnu_df = df[df['ds'] == df['regtime']].groupby('platform').user_id.nunique().reset_index().rename(columns={'user_id': 'wnu'})
    # print dnu_df.head(10)
    pay_num_df = df[df['pay'] != 0].groupby('platform').user_id.nunique().reset_index().rename(columns={'user_id': 'pay_num'})
    # print pay_num_df.head(10)
    pay_df = df.groupby('platform').pay.sum().reset_index()
    # print pay_df.head(10)
    data_df = (dau_df.merge(dnu_df, on='platform', how='left').merge(pay_num_df, on='platform', how='left')
               .merge(pay_df, on='platform', how='left').fillna(0))

    # print data_df
    return data_df


def platform_rank_data(df1,df2):
    df2 = df2.rename(columns={'wau': 'wau_ago', 'wnu': 'wnu_ago', 'pay_num': 'pay_num_ago', 'pay': 'pay_ago'})
    df2['rank_ago'] = df2['pay_ago'].rank(method='first', ascending=False)
    df2['arpu_ago'] = df2['pay_ago']*1.0/df2['wau_ago']
    print df2.head(10)
    df1 = df1.sort_values(by='pay', ascending=False)
    df1['rank'] = range(1, len(df1) + 1)
    df1['arpu'] = df1['pay'] * 1.0 / df1['wau']
    df = df1.merge(df2, on='platform', how='left')
    print df.head(10)
    result_df_1 = df[df['rank'] <= 9]
    result_df_2 = df[df['rank'] >= 9]
    result_df_2['platform'] = 'else'
    result_df_2 = result_df_2.groupby('platform').agg({'wau': 'sum', 'wnu': 'sum', 'pay_num': 'sum', 'pay': 'sum', 'rank': 'sum', 'arpu': 'sum',
                                                       'wau_ago': 'sum', 'wnu_ago': 'sum', 'pay_num_ago': 'sum', 'pay_ago': 'sum',
                                                       'rank_ago': 'sum', 'arpu_ago': 'sum'}).reset_index()
    result_df = pd.concat([result_df_1, result_df_2])
    result_df['huan_bi'] = (result_df['pay'] - result_df['pay_ago'])*1.0/result_df['pay_ago']
    columns = ['platform', 'wau_ago', 'wau', 'wnu_ago', 'wnu', 'pay_num_ago', 'pay_num', 'pay', 'pay_ago', 'huan_bi', 'arpu', 'arpu_ago', 'rank_ago', 'rank']
    result_df = result_df[columns]
    print result_df.head(10)
    return result_df


if __name__ == '__main__':

    week_date = time.strftime("%w")
    week = time.strftime("%W")
    print '第' + week + '周'
    print '周' + week_date
    if week_date == '4':
        settings_dev.set_env('dancer_pub')
        today = str(datetime.date.today()).replace('-', '')
        date = ds_add(today, -1)
        date_ago = ds_add(date, -7)
        this_week_df = weekly_data(date)
        last_week_df = weekly_data(date_ago)
        result = platform_rank_data(this_week_df, last_week_df)
        result.to_excel(r'E:\Data\output\report\daily_date_7_platform_dancer%s.xlsx'%week)