#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 月卡、季卡、周卡、月初基金购买人数
Time        : 2017.04.14
illustration: 季卡：'19'、周卡：'2'、月卡：'1'
# 月初基金的接口
month_foundation.index_foundation 首页
month_foundation.activate 激活（表示购买月初基金）
month_foundation.withdraw 领奖
'''
import settings_dev
from utils import hqls_to_dfs
from utils import update_mysql
from sqls_for_games.superhero import gs_sql

product_dic = {'1': 'month', '2': 'week', '19': 'quarter'}


def dis_revenu_buy(date):
    pay_sql = '''
    SELECT uid AS user_id,
           substr(uid,1,1) as plat,
           product_id
    FROM raw_paylog
    WHERE ds = '{date}'
      AND platform_2 <> 'admin_test'
      AND product_id in ('1', '2', '19')
    '''.format(date=date)
    action_sql = '''
    SELECT uid AS user_id,
           substr(uid,1,1) as plat
    FROM raw_action_log
    WHERE ds = '{date}'
      AND action = 'month_foundation.activate'
    '''.format(date=date)
    pay_df, gs_df, action_df = hqls_to_dfs([pay_sql, gs_sql, action_sql])
    # 排除开服至今的gs数据
    pay_df = pay_df[~pay_df['user_id'].isin(gs_df.user_id.values)]
    action_df = action_df[~action_df['user_id'].isin(gs_df.user_id.values)]
    result_df = (pay_df.groupby(
        ['product_id', 'plat']).count().reset_index()
        .pivot_table('user_id', ['plat'], 'product_id')
        .reset_index()
        .rename(columns=product_dic)
    )
    if len(action_df) == 0:
        result_df['foundation'] = 0
    else:
        action_result_df = (action_df.groupby('plat').count(
        ).reset_index().rename(columns={'user_id': 'foundation'}))
        result_df = result_df.merge(action_result_df, on='plat')
    result_df['ds'] = date

    # 更新MySQL表
    table = 'dis_revenu_buy'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    column = ['ds', 'week', 'month', 'quarter', 'foundation']

    if settings_dev.code == 'superhero_bi':
        pub_result_df = result_df[result_df.plat == 'g'][column]
        ios_result_df = result_df[result_df.plat == 'a'][column]
        update_mysql(table, pub_result_df, del_sql, 'superhero_pub')
        update_mysql(table, ios_result_df, del_sql, 'superhero_ios')
    else:
        update_mysql(table, result_df[column], del_sql)

    print 'dis_revenu_buy is complete'


if __name__ == '__main__':
    settings_dev.set_env('superhero_bi')
    date = '20170406'
    dis_revenu_buy(date)
