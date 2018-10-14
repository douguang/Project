#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import settings
import pandas as pd
from utils import ds_add, hql_to_df, update_mysql, get_config, hqls_to_dfs, date_range

def formatDate(Date, formatType='YYYY-MM-DD'):
    formatType = formatType.replace('YYYY', Date[0:4])
    formatType = formatType.replace('MM', Date[4:6])
    formatType = formatType.replace('DD', Date[-2:])
    return formatType

def month_arpu(date):
    reg_time = formatDate(date)
    sql = '''
    SELECT t1.uid user_id,
           reg_time,
           login_time,
           pay_time,
           pay_daily
    FROM
      (SELECT uid,
              to_date(create_time) as reg_time,
              ds as login_time
       FROM raw_info
       WHERE ds >= '{date}'
         AND ds <= '{month}'
         and substr(uid,1,1) = 'g'
         AND to_date(create_time) = '{reg_time}') t1
    LEFT OUTER JOIN
      (SELECT uid,
              ds AS pay_time,
              sum(order_money) AS pay_daily
       FROM raw_paylog
       WHERE ds >= '{date}'
         AND ds <= '{month}'
         and substr(uid,1,1) = 'g'
         and platform_2 <> 'admin_test'
       GROUP BY uid,ds) t2 ON t1.uid = t2.uid AND t2.pay_time = t1.login_time
     '''.format(**{'date': date,
                  'reg_time': reg_time,
                  'month': ds_add(date, 29)})
    print sql
    df = hql_to_df(sql)
    df['reg_time'] = pd.to_datetime(df['reg_time'])
    df['login_time'] = pd.to_datetime(df['login_time'])
    df['pay_time'] = pd.to_datetime(df['pay_time'])
    print df
    return df
if __name__ == '__main__':
    result_df = []
    settings.set_env('superhero_bi')
    for days in date_range('20160301','20160331'):
        result_df.append(month_arpu(days))
    result_dfs = pd.concat(result_df)
    result_dfs.to_excel('/Users/kaiqigu/Downloads/Excel/superhero_tw_30_day_arpu.xlsx')
