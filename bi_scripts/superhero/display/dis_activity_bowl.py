#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 活动 - 聚宝盆活动
Time        : 2017.04.10
illustration: 1：经典，2：豪华，3：至尊
台湾和越南：无聚宝盆活动
'''
import settings_dev
from utils import hqls_to_dfs
from utils import update_mysql
from utils import get_active_conf
from utils import date_range
from sqls_for_games.superhero import gs_sql


sort_dic = {'1': 'common', '2': 'luxury', '3': 'extreme'}


def dis_activity_bowl(date):
    if settings_dev.code in ['superhero_tw', 'superhero_vt']:
        return
    version, act_start_time, act_end_time = get_active_conf('bowl', date)
    if not version:
        return
    bowl_sql = '''
    SELECT ds,
           uid as user_id,
           substr(uid,1,1) as plat,
           args
    FROM raw_action_log
    WHERE ds='{date}'
      AND action = 'bowl.choice'
    '''.format(date=date)
    bowl_df, gs_df = hqls_to_dfs([bowl_sql, gs_sql])
    # 排除开服至今的测试用户的数据
    bowl_result_df = bowl_df[~bowl_df['user_id'].isin(gs_df.user_id.values)]
    bowl_result_df = bowl_result_df.copy()
    bowl_result_df['sort'] = bowl_result_df.args.map(
        lambda s: eval(str(s))['sort'][0])

    bowl_result_df['num'] = bowl_result_df.args.map(
        lambda s: eval(str(s))['num'][0])
    bowl_result_df['num'] = bowl_result_df.num.astype('int')
    result_df = (
        bowl_result_df.groupby(['ds', 'sort', 'plat']).sum().reset_index()
        .rename(columns={'num': 'times'})
        .pivot_table('times', ['ds', 'plat'], 'sort')
        .reset_index().rename(columns=sort_dic))
    # 更新MySQL
    table = 'dis_activity_bowl'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    column = ['ds', 'common', 'luxury', 'extreme']

    if settings_dev.code == 'superhero_bi':
        pub_result_df = result_df[result_df.plat == 'g'][column]
        ios_result_df = result_df[result_df.plat == 'a'][column]
        update_mysql(table, pub_result_df, del_sql, 'superhero_pub')
        update_mysql(table, ios_result_df, del_sql, 'superhero_ios')
    else:
        update_mysql(table, result_df[column], del_sql)

    print 'dis_activity_bowl complete'


if __name__ == '__main__':
    settings_dev.set_env('superhero_qiku')
    for date in date_range('20170402', '20170411'):
        print date
        dis_activity_bowl(date)
