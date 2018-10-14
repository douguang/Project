#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 用户 - VIP等级分布
Time        : 2017.04.28
illustration:
'''
import settings_dev
from utils import hql_to_df
from utils import update_mysql


def dis_vip_level_dst(date):
    assist_sql = '''
    SELECT vip as vip_level,
        plat,
        is_new_pay,
        count(user_id) as user_num,
        sum(order_money) as sum_money,
        sum(order_coin) as sum_coin
    FROM mart_assist
    WHERE ds = '{date}'
    AND vip > 0
    GROUP BY vip,plat,is_new_pay
    '''.format(date=date)
    assist_df = hql_to_df(assist_sql)
    assist_df = assist_df.drop_duplicates()
    # 总数据
    total_columns = ['plat', 'vip_level', 'income', 'vip_user_total']
    total_dic = {'user_num': 'vip_user_total', 'sum_money': 'income'}
    total_df = assist_df.groupby(
        ['plat', 'vip_level']).sum().reset_index().rename(
            columns=total_dic)[total_columns]
    # 新增VIP用户数据
    new_columns = ['plat', 'vip_level', 'new_vip_user', 'new_vip_pay_coin']
    new_dic = {'user_num': 'new_vip_user', 'sum_coin': 'new_vip_pay_coin'}
    new_df = assist_df[assist_df.is_new_pay == 1].rename(
        columns=new_dic)[new_columns]
    # 老VIP用户数据
    old_columns = ['plat', 'vip_level', 'old_vip_user', 'old_vip_pay_coin']
    old_dic = {'user_num': 'old_vip_user', 'sum_coin': 'old_vip_pay_coin'}
    old_df = assist_df[assist_df.is_new_pay == 0].rename(
        columns=old_dic)[old_columns]
    # 数据汇总
    result_df = (
        total_df.merge(new_df, on=['plat', 'vip_level'],
                       how='left')
        .merge(old_df, on=['plat', 'vip_level'],
               how='left').fillna(0))
    # 新增vip登陆占比
    result_df['new_vip_login_rate'] = result_df['new_vip_user'] * \
        1.0 / result_df['vip_user_total']
    result_df['new_vip_pay_rate'] = result_df['new_vip_pay_coin'] * \
        1.0 / (result_df['new_vip_pay_coin'] + result_df['old_vip_pay_coin'])
    result_df['ds'] = date
    result_df = result_df.fillna(0)

    # 更新MySQL
    table = 'dis_vip_level_dst'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    column = ['ds', 'vip_level', 'income', 'new_vip_user', 'old_vip_user',
              'vip_user_total', 'new_vip_pay_coin', 'old_vip_pay_coin',
              'new_vip_login_rate', 'new_vip_pay_rate']

    if settings_dev.code == 'superhero_bi':
        pub_result_df = result_df[result_df.plat == 'g'][column]
        ios_result_df = result_df[result_df.plat == 'a'][column]
        update_mysql(table, pub_result_df, del_sql, 'superhero_pub')
        update_mysql(table, ios_result_df, del_sql, 'superhero_ios')
    else:
        update_mysql(table, result_df[column], del_sql)

    print '{table} complete'.format(table=table)


if __name__ == '__main__':
    settings_dev.set_env('superhero_tw')
    date = '20170427'
    dis_vip_level_dst(date)
