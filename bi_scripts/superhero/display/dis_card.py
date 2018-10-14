#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 卡牌 - 卡牌转生、进阶、超进化
Time        : 2017.04.24
illustration:
'''
import settings_dev
from utils import hqls_to_dfs
from utils import update_mysql
from utils import date_range
from sqls_for_games.superhero import gs_sql
from sqls_for_games.superhero import super_sql
import pandas as pd

def update_data(result_df, table, column, date):
    # 更新MySQL
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)

    if settings_dev.code == 'superhero_bi':
        pub_result_df = result_df[result_df.plat == 'g'][column]
        ios_result_df = result_df[result_df.plat == 'a'][column]
        update_mysql(table, pub_result_df, del_sql, 'superhero_pub')
        update_mysql(table, ios_result_df, del_sql, 'superhero_ios')
    else:
        update_mysql(table, result_df[column], del_sql)

    print '{table} complete'.format(table=table)


def dis_card(date):
    card_sql = '''
    SELECT card_id,
           card_name,
           user_id,
           vip,
           is_fight,
           zhuansheng,
           jinjie,
           plat
    FROM mart_card
    WHERE ds ='{date}'
    '''.format(date=date)
    assist_sql = '''
    SELECT user_id,
        vip
    FROM mart_assist
    WHERE ds ='{date}'
    '''.format(date=date)
    card_df, super_df, gs_df, assist_df = hqls_to_dfs(
        [card_sql, super_sql.format(date=date), gs_sql, assist_sql])
    assist_df = pd.DataFrame(assist_df).drop_duplicates()
    total_dic = {'zhuansheng': 'total_num',
                 'user_id': 'have_user_num',
                 'is_fight': 'attend_num'}
    # 卡牌总张数、拥有人数、上阵人数
    total_df = (card_df.groupby(['card_id', 'card_name', 'plat', 'vip']).agg({
        'zhuansheng': 'count',
        'user_id': 'nunique',
        'is_fight': 'sum',
    }).reset_index().rename(columns=total_dic))
    total_df['use_rate'] = total_df['attend_num'] * \
        1.0 / total_df['have_user_num']
    total_df['ds'] = date

    # ======================================================================
    zhuangsheng_num = [0, 1, 2, 3, 4, 5, 6, 7]
    zhuangsheng_dic = {num: 'd%d_relive_num' % num for num in zhuangsheng_num}
    # 转生
    zhuangsheng_df = (card_df.groupby(
        ['card_id', 'plat', 'zhuansheng', 'vip']).count().user_id.reset_index(
        ).pivot_table('user_id', ['card_id', 'plat', 'vip'], 'zhuansheng')
                      .reset_index().fillna(0).rename(columns=zhuangsheng_dic))
    # 补充不存在的转生数据
    for i in ['d%d_relive_num' % num for num in zhuangsheng_num]:
        if i not in zhuangsheng_df.columns:
            zhuangsheng_df[i] = 0

    # 综合卡牌转生数据
    zhuansheng_result_df = total_df.merge(zhuangsheng_df,
                                          on=['card_id', 'plat', 'vip'],
                                          how='left').fillna(0)

    # ======================================================================
    jinjie_num = [i for i in range(0, 61)]
    jinjie_dic = {num: 'd%d_evo_num' % num for num in jinjie_num}
    # 进阶
    jinjie_df = (card_df.groupby(
        ['card_id', 'plat', 'jinjie', 'vip']).count().user_id.reset_index()
                 .pivot_table('user_id', ['card_id', 'plat', 'vip'], 'jinjie')
                 .reset_index().fillna(0).rename(columns=jinjie_dic))
    # 补充不存在的进阶数据
    for i in ['d%d_evo_num' % num for num in jinjie_num]:
        if i not in jinjie_df.columns:
            jinjie_df[i] = 0
    # 综合卡牌进阶数据
    jinjie_result_df = total_df.merge(jinjie_df,
                                      on=['card_id', 'plat', 'vip'],
                                      how='left').fillna(0)

    # ======================================================================
    super_num = [i for i in range(0, 9)]
    super_dic = {num: 'd%d_super_num' % num for num in super_num}
    # 加入VIP等级
    super_df = super_df.merge(assist_df, on='user_id')
    # 排除卡牌超进化的测试用户
    super_df = super_df[~super_df['user_id'].isin(gs_df.user_id.values)]
    # # 保留神卡
    # super_df = super_df[super_df['card_id'].isin(
    #     total_df.card_id.values)]
    # 超进化
    super_result = (
        super_df.groupby(['card_id', 'plat', 'super_step_level', 'vip']).count(
        ).user_id.reset_index().pivot_table(
            'user_id', ['card_id', 'plat', 'vip'], 'super_step_level')
        .reset_index().fillna(0).rename(columns=super_dic))
    # 补充不存在的超进化数据
    for i in ['d%d_super_num' % num for num in super_num]:
        if i not in super_result.columns:
            super_result[i] = 0

    # 综合卡牌超进化数据
    super_result_df = total_df.merge(super_result,
                                     on=['card_id', 'plat', 'vip'],
                                     how='left').fillna(0)

    # ======================================================================
    # 更新MySQL
    zhuansheng_table = 'dis_card_relive'
    jinjie_table = 'dis_card_evo'
    super_table = 'dis_card_super'
    zhuansheng_column = ['ds', 'vip', 'card_id', 'card_name', 'total_num',
                         'have_user_num', 'attend_num', 'use_rate',
                         'd0_relive_num', 'd1_relive_num', 'd2_relive_num',
                         'd3_relive_num', 'd4_relive_num', 'd5_relive_num',
                         'd6_relive_num', 'd7_relive_num']
    jinjie_column = ['ds', 'vip', 'card_id', u'card_name', 'total_num',
                     'have_user_num', 'attend_num', 'use_rate'
                     ] + ['d%d_evo_num' % num for num in jinjie_num]
    super_column = ['ds', 'vip', 'card_id', u'card_name', 'total_num',
                    'have_user_num', 'attend_num', 'use_rate'
                    ] + ['d%d_super_num' % num for num in super_num]

    update_data(zhuansheng_result_df, zhuansheng_table, zhuansheng_column,
                date)
    update_data(jinjie_result_df, jinjie_table, jinjie_column, date)
    update_data(super_result_df, super_table, super_column, date)


if __name__ == '__main__':
    for platform in ['superhero_bi']:
        for date in date_range('20171001', '20171015'):
            settings_dev.set_env(platform)
            print date, platform
            dis_card(date)
