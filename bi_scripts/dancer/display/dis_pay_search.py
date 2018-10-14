#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-11-30 下午12:25
@Author  : Andy
@File    : dis_pay_search.py
@Software: PyCharm
Description :  客服查数页面
'''
from utils import hql_to_df, ds_add, update_mysql
from utils import hql_to_df, date_range, ds_add
import settings_dev
import pandas as pd
import time


def dis_pay_search(date):
    pay_sql = '''
    SELECT ds,
           user_id,
           count(order_id) AS times,
           sum(order_money) AS pay
    FROM raw_paylog
    WHERE ds='{date}'
      AND platform_2 <> 'admin_test'
    GROUP BY user_id,
             ds
    '''.format(date=date)
    result = hql_to_df(pay_sql)
    table = 'dis_pay_search'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result, del_sql)


if __name__ == '__main__':
    for platform in ['dancer_pub', ]:
        settings_dev.set_env(platform)
        for date in date_range('20161104', '20170618'):
            print date
            result = dis_pay_search(date)
    print "end"
