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

def weekly_data(platform, date):

    # 数据主体
    sql = '''
    select '{platform}' as youxi,t1.user_id, t1.vip, t2.pay from (
        select user_id, vip from mid_info_all where ds='{date}' and VIP>0 and regexp_replace(substr(act_time,1,10),'-','')>='{date_7}') t1
        left join (
        select user_id, sum(order_money) as pay from raw_paylog where ds>='{date_7}' and ds<='{date}' and platform_2<>'admin_test' and order_id not like '%testktwwn%' group by user_id) t2
        on (t1.user_id= t2.user_id)
    '''.format(date=date, date_7=ds_add(date, -6), platform=platform)

    # if platform in ('dancer_pub', 'dancer_tw'):
    #     sql = sql.replace('raw_info', 'parse_info')
    print sql

    df = hql_to_df(sql).fillna(0)
    print df.head(10)
    dau_df = df.groupby(['youxi', 'vip']).user_id.nunique().reset_index().rename(columns={'user_id': 'wau'})
    print dau_df.head(10)
    # dnu_df = df[df['ds'] == df['regtime']].groupby(['youxi', 'vip']).user_id.nunique().reset_index().rename(columns={'user_id': 'wnu'})
    # print dnu_df.head(10)
    pay_num_df = df[df['pay'] != 0].groupby(['youxi', 'vip']).user_id.nunique().reset_index().rename(columns={'user_id': 'pay_num'})
    print pay_num_df.head(10)
    pay_df = df.groupby(['youxi', 'vip']).pay.sum().reset_index()
    print pay_df.head(10)
    data_df = (dau_df.merge(pay_num_df, on=['youxi', 'vip'], how='left').merge(pay_df, on=['youxi', 'vip'], how='left').fillna(0))
    print data_df

    # 转化人数
    sql_t_now = '''
        select '{platform}' as youxi, vip, count(distinct user_id) as transform_now from mid_info_all where ds='{date}' and vip>=1 group by vip order by vip
    '''.format(date=date, platform=platform)
    df_t_now = hql_to_df(sql_t_now)
    sql_t_ago = '''
        select '{platform}' as youxi, vip, count(distinct user_id) as transform_ago from mid_info_all where ds='{date_7}' and vip>=1 group by vip order by vip
    '''.format(date_7=ds_add(date, -7), platform=platform)
    df_t_ago = hql_to_df(sql_t_ago)
    df_t = df_t_now.merge(df_t_ago, on=['youxi', 'vip'], how='left')
    df_t['transform'] = df_t['transform_now'] - df_t['transform_ago']
    del df_t['transform_now']
    del df_t['transform_ago']

    data_df = data_df.merge(df_t, on=['youxi', 'vip'], how='left')
    return data_df


def vip_combine_data(df1, df2):
    df2 = df2.rename(columns={'wau': 'wau_ago', 'transform': 'transform_ago', 'pay_num': 'pay_num_ago', 'pay': 'pay_ago'})
    df = df1.merge(df2, on=['youxi', 'vip'], how='left')
    vip_dic = {1: '01-04', 2: '01-04', 3: '01-04', 4: '01-04', 5: '05-09', 6: '05-09', 7: '05-09', 8: '05-09',
               9: '05-09', 10: '10-12', 11: '10-12', 12: '10-12', 13: '13-14', 14: '13-14', 15: '15'}
    df['vip'] = df.vip.replace(vip_dic)
    df = df.groupby(['youxi', 'vip']).agg({'wau': 'sum', 'transform': 'sum', 'pay_num': 'sum', 'pay': 'sum', 'wau_ago': 'sum', 'transform_ago': 'sum', 'pay_num_ago': 'sum', 'pay_ago': 'sum'}).reset_index()
    df['arppu'] = df['pay']*1.0/df['pay_num']
    df['arppu_ago'] = df['pay_ago'] * 1.0 / df['pay_num_ago']

    df['wau_rate'] = (df['wau'] - df['wau_ago'])*1.0/df['wau_ago']
    df['pay_num_rate'] = (df['pay_num'] - df['pay_num_ago'])*1.0/df['pay_num_ago']
    df['transform_rate'] = (df['transform'] - df['transform_ago'])*1.0/df['transform_ago']
    df['arppu_rate'] = (df['arppu'] - df['arppu_ago'])*1.0/df['arppu_ago']

    columns = ['youxi', 'vip', 'wau_ago', 'wau', 'wau_rate', 'pay_num_ago', 'pay_num', 'pay_num_rate', 'transform_ago', 'transform', 'transform_rate', 'arppu_ago', 'arppu', 'arppu_rate', 'pay_ago', 'pay']
    df =df[columns]
    return df


if __name__ == '__main__':

    week_date = time.strftime("%w")
    week = time.strftime("%W")
    print '第' + week + '周'
    print '周' + week_date
    if week_date == '4':
        today = str(datetime.date.today()).replace('-', '')
        date = ds_add(today, -1)
        date_ago = ds_add(date, -7)
        daily_date_14 = []
        daily_date_14_combine = []
        for platform in ['dancer_pub', 'dancer_tw', 'dancer_bt']:
            settings_dev.set_env(platform)
            this_week_df = weekly_data(platform, date)
            daily_date_14.append(this_week_df)
            last_week_df = weekly_data(platform, date_ago)

            combine_df = vip_combine_data(this_week_df, last_week_df)
            daily_date_14_combine.append(combine_df)

        result = pd.concat(daily_date_14)
        result.to_excel(r'E:\Data\output\report\daily_date_7_vip_dancer%s.xlsx'%week)
        combine_result = pd.concat(daily_date_14_combine)
        combine_result.to_excel(r'E:\Data\output\report\daily_date_7_vip_dancer%s_combine.xlsx'%week)
