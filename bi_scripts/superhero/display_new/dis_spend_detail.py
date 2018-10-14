#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 分接口钻石消费
Time        : 2017.04.11
illustration:
'''
import settings_dev
from utils import hqls_to_dfs
from utils import update_mysql
from sqls_for_games.superhero import gs_sql


def dis_spend_detail(date):
    # 当日用户消费钻石
    spend_sql = '''
    SELECT uid as user_id,
           goods_type as api,
           substr(uid,1,1) AS plat,
           sum(coin_num) AS spend_coin
    FROM raw_spendlog
    WHERE ds ='{date}'
    GROUP BY uid,
             goods_type
    '''.format(date=date)
    spend_df, gs_df = hqls_to_dfs([spend_sql, gs_sql])
    # 排除开服至今的gs数据
    spend_result_df = spend_df[~spend_df['user_id'].isin(gs_df.user_id.values)]

    result_df = (spend_result_df.groupby(
        ['api', 'plat']).sum().spend_coin.reset_index())
    result_df['ds'] = date

    # 更新MySQL表
    table = 'dis_spend_detail'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    column = ['ds', 'api', 'spend_coin']

    if settings_dev.code == 'superhero_bi':
        pub_result_df = result_df[result_df.plat == 'g'][column]
        ios_result_df = result_df[result_df.plat == 'a'][column]
        update_mysql(table, pub_result_df, del_sql, 'superhero_pub')
        update_mysql(table, ios_result_df, del_sql, 'superhero_ios')
    else:
        update_mysql(table, result_df[column], del_sql)

    print 'dis_spend_detail is complete'


if __name__ == '__main__':
    settings_dev.set_env('superhero_bi')
    date = '20160717'
    dis_spend_detail(date)
