#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 充值档次分布
create_date : 2016.07.18
'''
import settings_dev
from utils import hql_to_df, update_mysql, date_range


def dis_pay_detail_level(date, plat=None):
    plat = plat or settings_dev.platform
    table = 'dis_pay_detail_level'
    sql = '''
    SELECT '{date}' AS ds,
           sum(CASE WHEN t1.pay = 1 THEN 1 ELSE 0 END) AS rmb1,
           sum(CASE WHEN t1.pay >= 2
               AND t1.pay <= 5 THEN 1 ELSE 0 END) AS rmb2_5,
           sum(CASE WHEN t1.pay = 6 THEN 1 ELSE 0 END) AS rmb6,
           sum(CASE WHEN t1.pay >= 7
               AND t1.pay <= 29 THEN 1 ELSE 0 END) AS rmb7_29,
           sum(CASE WHEN t1.pay = 30 THEN 1 ELSE 0 END) AS rmb30,
           sum(CASE WHEN t1.pay >= 31
               AND t1.pay <= 49 THEN 1 ELSE 0 END) AS rmb31_49,
           sum(CASE WHEN t1.pay = 50 THEN 1 ELSE 0 END) AS rmb50,
           sum(CASE WHEN t1.pay >= 51
               AND t1.pay <= 99 THEN 1 ELSE 0 END) AS rmb51_99,
           sum(CASE WHEN t1.pay = 100 THEN 1 ELSE 0 END) AS rmb100,
           sum(CASE WHEN t1.pay >= 101
               AND t1.pay <= 299 THEN 1 ELSE 0 END) AS rmb101_299,
           sum(CASE WHEN t1.pay = 300 THEN 1 ELSE 0 END) AS rmb300,
           sum(CASE WHEN t1.pay >= 301
               AND t1.pay <= 499 THEN 1 ELSE 0 END) AS rmb301_499,
           sum(CASE WHEN t1.pay = 500 THEN 1 ELSE 0 END) AS rmb500,
           sum(CASE WHEN t1.pay >= 501
               AND t1.pay <= 799 THEN 1 ELSE 0 END) AS rmb501_799,
           sum(CASE WHEN t1.pay = 800 THEN 1 ELSE 0 END) AS rmb800,
           sum(CASE WHEN t1.pay >= 801
               AND t1.pay <= 999 THEN 1 ELSE 0 END) AS rmb801_999,
           sum(CASE WHEN t1.pay = 1000 THEN 1 ELSE 0 END) AS rmb1000,
           sum(CASE WHEN t1.pay >= 1001
               AND t1.pay <= 1499 THEN 1 ELSE 0 END) AS rmb1001_1499,
           sum(CASE WHEN t1.pay = 1500 THEN 1 ELSE 0 END) AS rmb1500,
           sum(CASE WHEN t1.pay >= 1501
               AND t1.pay <= 1999 THEN 1 ELSE 0 END) AS rmb1501_1999,
           sum(CASE WHEN t1.pay = 2000 THEN 1 ELSE 0 END) AS rmb2000,
           sum(CASE WHEN t1.pay >= 2001
               AND t1.pay <= 2999 THEN 1 ELSE 0 END) AS rmb2001_2999,
           sum(CASE WHEN t1.pay = 3000 THEN 1 ELSE 0 END) AS rmb3000,
           sum(CASE WHEN t1.pay >= 3001
               AND t1.pay <= 3999 THEN 1 ELSE 0 END) AS rmb3001_3999,
           sum(CASE WHEN t1.pay = 4000 THEN 1 ELSE 0 END) AS rmb4000,
           sum(CASE WHEN t1.pay >= 4001
               AND t1.pay <= 4999 THEN 1 ELSE 0 END) AS rmb4001_4999,
           sum(CASE WHEN t1.pay >= 5001 THEN 1 ELSE 0 END) AS rmb5001
    FROM
      ( SELECT user_id,
               sum(order_money) AS pay
       FROM raw_paylog
       WHERE ds = '{date}'
         AND platform_2<>'admin_test'
         AND order_id NOT LIKE '%test%'
       GROUP BY user_id ) t1
    '''.format(**{'date': date})

    if plat == 'dancer_tw':
        sql = sql.replace('sum(order_money)', 'sum(order_money)/5.0')
        # print sql

    df = hql_to_df(sql)
    df = df.fillna(0)
    print df.head(10)
    # 更新MySQL
    print date, table
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, df, del_sql)


if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    for date in date_range('20160907', '20170326'):
        dis_pay_detail_level(date)
