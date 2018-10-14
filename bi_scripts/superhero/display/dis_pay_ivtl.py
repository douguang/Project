#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 营收 - 充值档次分布
Time        : 2017.05.04
illustration:
'''
import settings_dev
import pandas as pd
from utils import hql_to_df
from utils import update_mysql

# 充值区间
ranges = [5, 6, 30, 50, 100, 500, 1000, 2000, 3000, 4000, 5000, 6000, 8000,
          500000]
# 台湾 - 充值区间
tw_ranges = [60, 300, 500, 1000, 5000, 10000, 20000, 30000, 40000, 50000,
             60000, 80000, 500000]
# 越南 - 充值区间
vt_ranges = [99, 100, 199, 200, 300, 500, 1500, 3000, 5000, 10000, 20000,
             30000, 40000, 50000, 50000000]


def update_data(result_df, table, column, date):
    # 更新MySQL
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)

    if settings_dev.code == 'superhero_bi':
        result_df = result_df.rename(columns={'vip': 'vip_level',
                                              'rmb5_6': 'rmb6',
                                              'rmb6_30': 'rmb7_30'})
        pub_result_df = result_df[result_df.plat == 'g'][column]
        ios_result_df = result_df[result_df.plat == 'a'][column]
        update_mysql(table, pub_result_df, del_sql, 'superhero_pub')
        update_mysql(table, ios_result_df, del_sql, 'superhero_ios')
    else:
        # if settings_dev.code == 'superhero_vt':
        #     result_df = result_df.rename(columns={'vip': 'vip_level'})
        if settings_dev.code == 'superhero_qiku':
            result_df = result_df.rename(columns={'vip': 'vip_level',
                                                  'rmb5_6': 'rmb6',
                                                  'rmb6_30': 'rmb7_30'})
        update_mysql(table, result_df[column], del_sql)

    print '{table} complete'.format(table=table)


def dis_pay_ivtl(date):
    assist_sql = '''
    SELECT ds,
           user_id,
           vip,
           plat,
           order_money,
           order_coin
    FROM mart_assist
    WHERE ds ='{date}'
      AND order_money > 0
    '''.format(date=date)
    assist_df = hql_to_df(assist_sql)

    assist_df = pd.DataFrame(assist_df).drop_duplicates()

    if settings_dev.code in ['superhero_tw', 'superhero_vt']:
        # 充值人数和充值金额汇总
        if settings_dev.code == 'superhero_tw':
            assist_df['ranges'] = pd.cut(assist_df.order_coin,
                                         tw_ranges).astype('object')
        if settings_dev.code == 'superhero_vt':
            assist_df['ranges'] = pd.cut(assist_df.order_coin,
                                         vt_ranges).astype('object')
        result_df = (assist_df.groupby(['vip', 'plat', 'ranges']).agg({
            'user_id': 'count',
            'order_coin': 'sum',
        }).reset_index().fillna(0).rename(columns={'user_id': 'pay_num'}))

        # 充值人数、充值金额
        pay_num_result = (result_df.pivot_table(
            'pay_num', ['plat', 'vip'], 'ranges').reset_index().fillna(0))
        pay_money_result = (result_df.pivot_table(
            'order_coin', ['plat', 'vip'], 'ranges').reset_index().fillna(0))

        # 补充不存在的数据
        if settings_dev.code == 'superhero_tw':
            categor_list = pd.cut(assist_df.order_coin,
                                  tw_ranges).cat.categories
        if settings_dev.code == 'superhero_vt':
            categor_list = pd.cut(assist_df.order_coin,
                                  vt_ranges).cat.categories
        for i in categor_list:
            if i not in pay_num_result.columns:
                pay_num_result[i] = 0
            if i not in pay_money_result.columns:
                pay_money_result[i] = 0
        # 增加日期，字段重命名
        dic_columns = {}
        for name in pay_num_result.columns:
            if ']' in name:
                dic_columns[name] = 'rmb' + str.strip(name[1:-1]).split(',')[
                    0] + '_' + str.strip(name[1:-1].split(',')[1])
        pay_num_result['ds'] = pay_money_result['ds'] = date
        pay_num_result = pay_num_result.rename(columns=dic_columns)
        pay_money_result = pay_money_result.rename(columns=dic_columns)
    else:
        # 充值人数和充值金额汇总
        assist_df['ranges'] = pd.cut(assist_df.order_money,
                                     ranges).astype('object')
        result_df = (assist_df.groupby(['vip', 'plat', 'ranges']).agg({
            'user_id': 'count',
            'order_money': 'sum',
        }).reset_index().fillna(0).rename(columns={'user_id': 'pay_num'}))

        # 充值人数、充值金额
        pay_num_result = (result_df.pivot_table(
            'pay_num', ['plat', 'vip'], 'ranges').reset_index().fillna(0))
        pay_money_result = (result_df.pivot_table(
            'order_money', ['plat', 'vip'], 'ranges').reset_index().fillna(0))

        # 补充不存在的数据
        for i in pd.cut(assist_df.order_money, ranges).cat.categories:
            if i not in pay_num_result.columns:
                pay_num_result[i] = 0
            if i not in pay_money_result.columns:
                pay_money_result[i] = 0

        # 增加日期，字段重命名
        dic_columns = {}
        for name in pay_num_result.columns:
            if ']' in name:
                dic_columns[name] = 'rmb' + str.strip(name[1:-1]).split(',')[
                    0] + '_' + str.strip(name[1:-1].split(',')[1])
        pay_num_result['ds'] = pay_money_result['ds'] = date
        pay_num_result = pay_num_result.rename(columns=dic_columns)
        pay_money_result = pay_money_result.rename(columns=dic_columns)

    # 更新MySQL
    table_num = 'dis_pay_ivtl_user_num'
    table_pay = 'dis_pay_ivtl_money'
    column = ['ds', 'vip_level', 'rmb6', 'rmb7_30', 'rmb30_50', 'rmb50_100',
              'rmb100_500', 'rmb500_1000', 'rmb1000_2000', 'rmb2000_3000',
              'rmb3000_4000', 'rmb4000_5000', 'rmb5000_6000', 'rmb6000_8000',
              'rmb8000_500000']
    tw_column = ['ds', 'vip', 'rmb60_300', 'rmb300_500', 'rmb500_1000',
                 'rmb1000_5000', 'rmb5000_10000', 'rmb10000_20000',
                 'rmb20000_30000', 'rmb30000_40000', 'rmb40000_50000',
                 'rmb50000_60000', 'rmb60000_80000', 'rmb80000_500000']
    vt_column = ['ds', 'vip', 'rmb99_100', 'rmb199_200', 'rmb300_500',
                 'rmb500_1500', 'rmb1500_3000', 'rmb3000_5000',
                 'rmb5000_10000', 'rmb10000_20000', 'rmb20000_30000',
                 'rmb30000_40000', 'rmb40000_50000', 'rmb50000_50000000']

    if settings_dev.code == 'superhero_tw':
        update_data(pay_num_result, table_num, tw_column, date)
        update_data(pay_money_result, table_pay, tw_column, date)
    elif settings_dev.code == 'superhero_vt':
        update_data(pay_num_result, table_num, vt_column, date)
        update_data(pay_money_result, table_pay, vt_column, date)
    else:
        update_data(pay_num_result, table_num, column, date)
        update_data(pay_money_result, table_pay, column, date)


if __name__ == '__main__':
    settings_dev.set_env('superhero_vt')
    date = '20170401'
    dis_pay_ivtl(date)
