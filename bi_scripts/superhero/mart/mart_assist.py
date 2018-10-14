#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 中间表综合数据,用户基本信息
Time        : 2017.04.07
'''
import settings_dev
from utils import hqls_to_dfs
from sqls_for_games.superhero import info_sql
from sqls_for_games.superhero import pay_sql
from sqls_for_games.superhero import spend_sql
from sqls_for_games.superhero import gs_sql
from sqls_for_games.superhero import reg_sql


def mart_assist(date):
    mart_paylog_sql = '''
    SELECT user_id,
        history_money,
        history_coin,
        is_new_pay
    FROM mart_paylog
    WHERE ds = '{date}'
    '''.format(date=date)
    info_df, pay_df, spend_df, reg_df, gs_df, mart_paylog_df = hqls_to_dfs(
        [info_sql.format(date=date), pay_sql.format(date=date),
         spend_sql.format(date=date), reg_sql.format(date=date), gs_sql, mart_paylog_sql])
    # 将未进入主城的用户的渠道填充缺失值为'None',其他数值填充为0
    assist_df = (
        info_df.merge(pay_df, on='user_id', how='left')
        .merge(mart_paylog_df, on='user_id', how='left')
        .merge(spend_df, on='user_id', how='left')
        .merge(reg_df, on=['user_id', 'plat'], how='outer').fillna({'platform': 'None'}).fillna(0))
    # 排除开服至今的gs数据
    result_df = assist_df[~assist_df['user_id'].isin(gs_df.user_id.values)]
    # 是否是新增充值用户(UID)，是：1，否：0
    result_df = result_df.copy()
    result_df['is_new_user'] = result_df['user_id'].isin(reg_df.user_id.values)
    result_df = result_df.replace({'is_new_user': {False: '0', True: '1'}})
    result_df['is_new_user'] = result_df['is_new_user'].astype('int')

    columns = ['user_id', 'name', 'server', 'platform', 'plat', 'account',
               'level', 'vip', 'reg_time', 'act_time', 'order_money', 'coin',
               'order_coin', 'spend_coin', 'history_money', 'history_coin',
               'is_new_pay', 'is_new_user']
    result_df = result_df[columns]

    return result_df


if __name__ == '__main__':
    settings_dev.set_env('superhero_bi')
    date = '20170421'
    result_df = mart_assist(date)
