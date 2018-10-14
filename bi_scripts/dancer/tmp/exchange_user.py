#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 渠道转移用户
'''
import pandas as pd
import settings_dev
from utils import hql_to_df, ds_add, update_mysql, format_dates, date_range


def exchange_user():
    settings_dev.set_env('dancer_pub')
    info_sql = '''
    SELECT account,
           t1.user_id,
           device_mark,
           reg_time,
           vip,
           order_money 
    FROM
      ( SELECT user_id, account, device_mark, reg_time, vip
       FROM mid_info_all
       WHERE ds = '20161123') t1
    LEFT OUTER JOIN
      ( SELECT user_id,
               sum(order_money) AS order_money
       FROM raw_paylog
       WHERE platform_2 != 'admin_test'
       GROUP BY user_id) t2 ON t1.user_id = t2.user_id
    '''
    print info_sql
    df = hql_to_df(info_sql)
    df.fillna(0)
    print df
    df.to_csv(
        r'C:\workflow\Temporary-demand\dancer\2016-11-24\old_exchange_user.csv')

if __name__ == '__main__':
    exchange_user()
