#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 营收 - 付费用户收入占比
Time        : 2017.04.12
illustration:
'''
import settings_dev
from utils import hql_to_df
from utils import update_mysql
import pandas as pd

range_list = [0, 10, 50, 100, 200, 500, 1000, 1000000]
columns_to_rename = {
    '(0, 10]': 'rmb0_10',
    '(10, 50]': 'rmb10_50',
    '(50, 100]': 'rmb50_100',
    '(100, 200]': 'rmb100_200',
    '(200, 500]': 'rmb200_500',
    '(500, 1000]': 'rmb500_1000',
    '(1000, 1000000]': 'rmb1000_1000000',
}


def dis_pay_rate(date):
    assist_sql = '''
    SELECT ds,
           plat,
           order_money
    FROM mart_assist
    WHERE ds ='{date}'
    AND order_money > 0
    '''.format(date=date)
    assist_df = hql_to_df(assist_sql)
    assist_df = assist_df.drop_duplicates()
    assist_df['ranges'] = pd.cut(assist_df.order_money,
                                 range_list).astype('object')
    categories_list = [cate
                       for cate in pd.cut(assist_df.order_money,
                                          range_list).cat.categories]
    pay_result = (assist_df.groupby(['ds', 'plat', 'ranges'])
                  .sum().reset_index().pivot_table('order_money',
                                                   ['ds', 'plat'], 'ranges')
                  .reset_index().fillna(0))
    # 补充category划分
    for cate in categories_list:
        if cate not in pay_result.columns:
            pay_result[cate] = 0
    pay_result = pay_result.rename(columns=columns_to_rename)
    pay_total_df = assist_df.groupby(
        ['ds', 'plat']).sum().reset_index().rename(
            columns={'order_money': 'pay_total'})
    result_df = pay_result.merge(pay_total_df, on=['ds', 'plat']).fillna(0)

    # 更新MySQL表
    table = 'dis_pay_rate'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    column = ['ds', 'rmb0_10', 'rmb10_50', 'rmb50_100', 'rmb100_200',
              'rmb200_500', 'rmb500_1000', 'rmb1000_1000000', 'pay_total']
    if settings_dev.code == 'superhero_bi':
        pub_result = result_df[result_df.plat == 'g'][column]
        ios_result = result_df[result_df.plat == 'a'][column]
        update_mysql(table, pub_result, del_sql, 'superhero_pub')
        update_mysql(table, ios_result, del_sql, 'superhero_ios')
    else:
        update_mysql(table, result_df[column], del_sql)

    print '{table} is complete'.format(table=table)


if __name__ == '__main__':
    settings_dev.set_env('superhero_qiku')
    date = '20170417'
    dis_pay_rate(date)
