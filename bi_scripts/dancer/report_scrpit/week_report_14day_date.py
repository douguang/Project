#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 周报 14日的每日数据
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
    sql = '''
    select '{platform}' as platform, 
                t1.ds, 
                t1.dau, 
                t2.dnu, 
                t3.pay_num, 
                t3.pay 
    from(
            select count(user_id) as dau, 
                        ds 
            from parse_info 
            where ds>='{date_14}' and ds<='{date}' 
            group by ds
            ) t1 
            left join 
            (
                select count(distinct user_id) as dnu, 
                        regexp_replace(substr(reg_time,1,10),'-','') as regtime 
                from parse_info 
                where ds>='{date_14}' and ds<='{date}' 
                group by regtime
                ) t2
            on t1.ds = t2.regtime 
            left join (
                            select count(user_id) as pay_num, 
                                sum(order_money) as pay, 
                                ds 
                            from raw_paylog 
                            where ds>='{date_14}' and ds<='{date}' and platform_2<>'admin_test' and order_id not like '%testktwwn%' 
                            group by ds
                            ) t3 
                            on t1.ds=t3.ds
    '''.format(date=date, date_14=ds_add(date, -13), platform=platform)
    print sql
    if platform in ('sanguo_ks', 'sanguo_tw', 'sanguo_tl'):
        sql = sql.replace('parse_info', 'raw_info')
    df = hql_to_df(sql)
    print df.head(10)
    return df


def weekly_total_data(platform, date):
    sql = '''
        select t1.platform, 
                t1.ds, 
                t1.wau, 
                t2.pay_num, 
                t2.pay 
        from(
                select '{platform}' as platform, 
                        '{date}' as ds, 
                        count(distinct user_id) as wau 
                from parse_info 
                where ds>='{date_6}' and ds<='{date}'
                ) t1 
                left join 
                (
                select '{platform}' as platform, 
                        '{date}' as ds, 
                        count(distinct user_id) as pay_num, 
                        sum(order_money) as pay 
                        from raw_paylog 
                        where ds>='{date_6}' and ds<='{date}' and platform_2 != 'admin_test'
         and order_id not like '%testktwwn%'
                ) t2 on (t1.ds=t2.ds and t1.platform=t2.platform)
    '''.format(platform=platform, date=date, date_6=ds_add(date, -6))
    print sql
    df = hql_to_df(sql)
    df['pay_rate'] = df['pay_num'] * 1.0 / df['wau']
    df['arpu'] = df['pay'] * 1.0 / df['wau']
    df['arppu'] = df['pay'] * 1.0 / df['pay_num']
    # df = df.drop(['wau', 'pay_num', 'pay'], inplace=True)         
    # inplace可选参数。如果手动设定为True（默认为False），那么原数组直接就被替换。
    del df['wau']
    del df['pay_num']
    del df['pay']
    return df


def weekly_total_data_duibi(platform, date):
    df1 = weekly_total_data(platform, date)
    date_ago = ds_add(date, -7)
    df2 = weekly_total_data(platform, date_ago)
    df2 = df2.rename(
        columns={
            'ds': 'ds_ago',
            'pay_rate': 'pay_rate_ago',
            'arpu': 'arpu_ago',
            'arppu': 'arppu_ago'})
    df = df1.merge(df2, on='platform', how='left')
    columns = [
        'platform',
        'ds_ago',
        'ds',
        'pay_rate_ago',
        'pay_rate',
        'arpu_ago',
        'arpu',
        'arppu_ago',
        'arppu']
    df = df[columns]
    print df
    return df


if __name__ == '__main__':

    week_date = time.strftime("%w")
    week = time.strftime("%W")
    print '第' + week + '周'
    print '周' + week_date
    if week_date == '2':
        today = str(datetime.date.today()).replace('-', '')
        date = ds_add(today, -1)

        # 每日数据
        daily_date_14 = []
        for platform in ['dancer_pub', 'dancer_tw', 'dancer_bt']:
            settings_dev.set_env(platform)
            daily_date_14.append(weekly_data(platform, date))
        result = pd.concat(daily_date_14)
        # result.to_excel(r'E:\Data\output\report\daily_date_14_dancer%s.xlsx'%week)

        # 综合对比
        daily_date_total_14 = []
        for platform in ['dancer_pub', 'dancer_tw', 'dancer_bt']:
            settings_dev.set_env(platform)
            daily_date_total_14.append(weekly_total_data_duibi(platform, date))
        result = pd.concat(daily_date_total_14)
        # result.to_excel(r'E:\Data\output\report\daily_date_14_dancer%s_duibi.xlsx'%week)
