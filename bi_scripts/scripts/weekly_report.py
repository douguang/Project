#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 周报
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


def weekly_data(platform, weekly_now, date_now):
    settings_dev.set_env(platform)
    income_sql = '''
    SELECT '{platform}' AS platform,
           'income',
           weekly_ago,
           income_ago,
           weekly_now,
           income_now
    FROM
      ( SELECT '{weekly_ago}' AS weekly_ago,
              sum(order_money) AS income_ago
       FROM raw_paylog
       WHERE ds >= '{date_14ago}'
         AND ds < '{date_7ago}'
         AND platform_2 != 'admin_test'
         and order_id not like '%testktwwn%') t1
    JOIN
      ( SELECT '{weekly_now}' AS weekly_now,
               sum(order_money) AS income_now
       FROM raw_paylog
       WHERE ds >= '{date_7ago}'
         AND ds <= '{date_now}'
         AND platform_2 != 'admin_test'
         and order_id not like '%testktwwn%') t2
    '''.format(**{
        'platform': platform,
        'weekly_now': weekly_now,
        'weekly_ago': weekly_now - 1,
        'date_7ago': ds_add(date_now, -6),
        'date_14ago': ds_add(date_now, -13),
        'date_now': date_now,
    })
    if platform in ('dancer_pub', 'dancer_tw', 'dancer_bt'):
        income_sql = income_sql.replace('raw_info', 'parse_info')
    income_df = hql_to_df(income_sql)
    print income_df

    wau_sql = '''
    SELECT '{platform}' AS platform,
           'wau',
           weekly_ago,
           wau_ago,
           weekly_now,
           wau_now
    FROM
      ( SELECT '{weekly_ago}' AS weekly_ago,
              count(DISTINCT user_id) AS wau_ago
       FROM raw_info
       WHERE ds >= '{date_14ago}'
         AND ds < '{date_7ago}') t1
    JOIN
      ( SELECT '{weekly_now}' AS weekly_now,
              count(DISTINCT user_id) AS wau_now
       FROM raw_info
       WHERE ds >= '{date_7ago}'
         AND ds <= '{date_now}') t2
    '''.format(**{
        'platform': platform,
        'weekly_now': weekly_now,
        'weekly_ago': weekly_now - 1,
        'date_7ago': ds_add(date_now, -6),
        'date_14ago': ds_add(date_now, -13),
        'date_now': date_now,
    })
    if platform in ('superhero_bi', 'superhero_vt'):
        wau_sql = wau_sql.replace('user_id', 'uid')
    if platform in ('dancer_pub', 'dancer_tw', 'dancer_bt'):
        wau_sql = wau_sql.replace('raw_info', 'parse_info')
    wau_df = hql_to_df(wau_sql)
    print wau_df

    newuser_sql = '''
    SELECT '{platform}' AS platform,
           'newuser',
           weekly_ago,
           newuser_ago,
           weekly_now,
           newuser_now
    FROM
      ( SELECT '{weekly_ago}' AS weekly_ago,
               count(DISTINCT user_id) AS newuser_ago
       FROM raw_info
       WHERE ds >= '{date_14ago}'
         AND ds < '{date_7ago}'
         AND to_date(reg_time) >= '{f_date_14ago}'
         AND to_date(reg_time) < '{f_date_7ago}') t1
    JOIN
      ( SELECT '{weekly_now}' AS weekly_now,
               count(DISTINCT user_id) AS newuser_now
       FROM raw_info
       WHERE ds >= '{date_7ago}'
         AND ds <= '{date_now}'
         AND to_date(reg_time) >= '{f_date_7ago}'
         AND to_date(reg_time) <= '{f_date_now}') t2
    '''.format(**{
        'platform': platform,
        'weekly_now': weekly_now,
        'weekly_ago': weekly_now - 1,
        'date_7ago': ds_add(date_now, -6),
        'date_14ago': ds_add(date_now, -13),
        'date_now': date_now,
        'f_date_7ago': formatDate(ds_add(date_now, -6)),
        'f_date_14ago': formatDate(ds_add(date_now, -13)),
        'f_date_now': formatDate(date_now),
    })
    if platform in ('superhero_bi', 'superhero_vt'):
        newuser_sql = newuser_sql.replace(
            'user_id', 'uid').replace('reg_time', 'create_time')
    if platform in ('dancer_pub', 'dancer_tw', 'dancer_bt'):
        newuser_sql = newuser_sql.replace('raw_info', 'parse_info')
    newuser_df = hql_to_df(newuser_sql)
    print newuser_df

if __name__ == '__main__':
    weekly_now = int(time.strftime('%W'))
    weeks_now = int(time.strftime('%w'))
    if weeks_now == 4:
        today = str(datetime.date.today()).replace('-', '')
    # weekly_now = 5
    # today = '20170202'
    date_now = ds_add(today, -1)
    print date_now
    for platform in ['superhero_bi', 'superhero_vt', 'sanguo_ks', 'sanguo_bt', 'sanguo_tl', 'dancer_pub',  'dancer_tw', 'dancer_bt']:
        weekly_data(platform, weekly_now, date_now)
