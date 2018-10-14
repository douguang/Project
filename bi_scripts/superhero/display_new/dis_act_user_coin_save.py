#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 活跃玩家钻石存量
Time        : 2017.04.10
illustration:
'''
import settings_dev
from utils import hql_to_df
from utils import update_mysql


def dis_act_user_coin_save(date):
    assist_sql = '''
    SELECT ds,
           user_id,
           plat,
           coin,
           spend_coin
    FROM mart_assist
    WHERE ds ='{date}'
    '''.format(date=date)
    assist_df = hql_to_df(assist_sql)
    assist_df = assist_df.drop_duplicates()
    # 当日活跃用户身上的钻石总数和消耗钻石总数
    result_df = (assist_df.groupby(['ds', 'plat']).agg({
        'user_id': 'count',
        'coin': 'sum',
        'spend_coin': 'sum'
    }).reset_index().rename(columns={'user_id': 'act_user_num',
                                     'coin': 'coin_save',
                                     'spend_coin': 'coin_spend'}))
    # 每位玩家平均钻石存量
    result_df['avg_coin_save'] = result_df['coin_save'].div(
        result_df['act_user_num'], fill_value=0)

    # 更新MySQL表
    table = 'dis_act_user_coin_save'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    column = ['ds', 'coin_save', 'coin_spend', 'act_user_num', 'avg_coin_save']

    if settings_dev.code == 'superhero_bi':
        pub_result_df = result_df[result_df.plat == 'g'][column]
        ios_result_df = result_df[result_df.plat == 'a'][column]
        update_mysql(table, pub_result_df, del_sql, 'superhero_pub')
        update_mysql(table, ios_result_df, del_sql, 'superhero_ios')
    else:
        update_mysql(table, result_df[column], del_sql)

    print 'dis_act_user_coin_save is complete'


if __name__ == '__main__':
    settings_dev.set_env('superhero_bi')
    date = '20170406'
    dis_act_user_coin_save(date)
