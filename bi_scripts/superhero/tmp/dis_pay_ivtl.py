#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 充值档次分布
'''
import settings
from utils import hql_to_df, update_mysql, date_range
import pandas as pd

def bi_sql(date, pp):
    bi_pay_sql = '''
    SELECT a.uid uid,
           a.vip_level vip_level,
           b.sum_money sum_money
    FROM
      (SELECT uid,
              vip_level
       FROM raw_info
       WHERE ds = '{date}'
       and substr(uid,1,1) ='{pp}' )a
    JOIN
      (SELECT uid,
              sum(order_money) sum_money
       FROM raw_paylog
       WHERE ds ='{date}'
         AND platform_2 <>'admin_test'
       GROUP BY uid)b ON a.uid = b.uid
    '''.format(date=date, pp=pp)

    return bi_pay_sql

def qiku_sql(date):
    qiku_pay_sql = '''
    SELECT a.uid uid,
           a.vip_level vip_level,
           b.sum_money sum_money
    FROM
      (SELECT uid,
              vip_level
       FROM raw_info
       WHERE ds = '{date}')a
    JOIN
      (SELECT uid,
              sum(order_money) sum_money
       FROM raw_paylog
       WHERE ds ='{date}'
         AND platform_2 <>'admin_test'
       GROUP BY uid)b ON a.uid = b.uid
    '''.format(date=date)

    return qiku_pay_sql

def dis_pay_ivtl_one(date,plat=None):
    plat = plat or settings.platform

    sql = '''
    SELECT a.uid uid,
           a.vip_level vip_level,
           b.sum_money sum_money
    FROM
      (SELECT uid,
              vip_level
       FROM raw_info
       WHERE ds = '{date}'
       and substr(uid,1,1) ='a' )a
    JOIN
      (SELECT uid,
              sum(order_money) sum_money
       FROM raw_paylog
       WHERE ds ='{date}'
         AND platform_2 <>'admin_test'
       GROUP BY uid)b ON a.uid = b.uid
    '''.format(date=date)

    if plat == 'superhero_pub':
        sql = bi_sql(date, 'g')
    if plat == 'superhero_ios':
        sql = bi_sql(date, 'a')
    if plat == 'superhero_qiku':
        sql = qiku_sql(date)

    df = hql_to_df(sql)

    ranges = [5, 6, 30, 50, 100, 500,1000,2000,3000,4000,5000,6000,8000,500000]
    columns_to_rename = {
        '(5, 6]': 'rmb6',
        '(6, 30]': 'rmb7_30',
        '(30, 50]': 'rmb30_50',
        '(50, 100]': 'rmb50_100',
        '(100, 500]': 'rmb100_500',
        '(500, 1000]': 'rmb500_1000',
        '(1000, 2000]': 'rmb1000_2000',
        '(2000, 3000]': 'rmb2000_3000',
        '(3000, 4000]': 'rmb3000_4000',
        '(4000, 5000]': 'rmb4000_5000',
        '(5000, 6000]': 'rmb5000_6000',
        '(6000, 8000]':'rmb6000_8000',
        '(8000, 500000]':'rmb8000_500000',
    }
    columns_to_show = ['ds','vip_level', 'rmb6', 'rmb7_30', 'rmb30_50',
                       'rmb50_100', 'rmb100_500','rmb500_1000','rmb1000_2000','rmb2000_3000'
                       ,'rmb3000_4000','rmb4000_5000','rmb5000_6000','rmb6000_8000','rmb8000_500000']
    result_df = (df.groupby(['vip_level',
                                        pd.cut(df.sum_money,
                                               ranges)])
                              .count()
                              .uid
                              .reset_index()
                              .pivot_table('uid', ['vip_level'], 'sum_money')
                              .reset_index()
                              .fillna(0)
                              .rename(columns=columns_to_rename))
    result_df['ds'] = date
    result_df = result_df[columns_to_show]

    # result_df.to_excel('/Users/kaiqigu/Downloads/Excel/pub.xlsx')

    #更新MySQL
    table = 'dis_pay_ivtl'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql, plat)

def dis_pay_ivtl(date):
    if settings.code == 'superhero_bi':
        for plat in ['superhero_pub', 'superhero_ios']:
        # for plat in ['superhero_ios']:
            print plat
            dis_pay_ivtl_one(date, plat)
    else:
        dis_pay_ivtl_one(date)

if __name__ == '__main__':
    settings.set_env('superhero_qiku')
    date = '20161009'
    dis_pay_ivtl(date)
