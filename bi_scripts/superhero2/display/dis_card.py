#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 卡牌进阶、升星。
'''
import settings_dev
import pandas as pd
from utils import ds_add, hql_to_df, update_mysql, get_config, date_range


def dis_card(date):

    card_sql = '''
        select t1.user_id, t1.server, t2.vip, t1.hero_id, t1.evo, t1.star from
        (select reverse(substr(reverse(user_id), 8)) as server, user_id, hero_id, evo, star from parse_hero where ds='{date}') t1
        left join
        (select user_id, vip from parse_info where ds='{date}') t2
        on t1.user_id=t2.user_id
    '''.format(date=date)
    card_df = hql_to_df(card_sql)

    info_config = get_config('hero_basis')
    card_df['evo'] = card_df['evo'].astype('int')
    card_df['star'] = card_df['star'].astype('int')
    card_df['ds'] = date
    columns = ['ds', 'user_id', 'server', 'vip', 'hero_id', 'evo', 'star']
    card_df = card_df[columns]

    #卡牌进阶
    card_have_num_df = card_df.groupby(['ds', 'server', 'vip', 'hero_id']).user_id.count().reset_index().rename(columns={'user_id': 'have_user_num'})
    evo_dic = {num: 'evo_%d' % num for num in range(1, 31, 1)}
    card_evo_df = pd.pivot_table(card_df, values='user_id', index=['ds', 'server', 'vip', 'hero_id'], columns='evo', aggfunc='count', fill_value=0).reset_index().rename(columns=evo_dic)
    for i in ['evo_%d'%num for num in range(1, 31, 1)]:
        if i not in card_evo_df.columns:
            card_evo_df[i] = 0
    card_evo_df = card_evo_df.merge(card_have_num_df, on=['ds', 'server', 'vip', 'hero_id'], how='left')
    card_evo_df['hero_name'] = card_evo_df['hero_id'].map(lambda x: info_config[str(x)]['name'])
    columns = ['ds', 'server', 'vip', 'hero_id', 'hero_name', 'have_user_num'] + ['evo_%d'%d for d in range(1, 31, 1)]
    card_evo_df = card_evo_df[columns]
    # print card_evo_df

    table = 'dis_card_evo'
    print date, table
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, card_evo_df, del_sql)

    # 卡牌升星
    star_dic = {num: 'star_%d' %num for num in range(1, 32, 1)}
    card_star_df = pd.pivot_table(card_df, values='user_id', index=['ds', 'server', 'vip', 'hero_id'], columns='star', aggfunc='count', fill_value=0).reset_index().rename(columns=star_dic)
    for i in ['star_%d'%num for num in range(1, 32, 1)]:
        if i not in card_star_df.columns:
            card_star_df[i] = 0
    card_star_df = card_star_df.merge(card_have_num_df, on=['ds', 'server', 'vip', 'hero_id'], how='left')
    card_star_df['hero_name'] = card_star_df['hero_id'].map(lambda x: info_config[str(x)]['name'])
    columns = ['ds', 'server', 'vip', 'hero_id', 'hero_name', 'have_user_num'] + ['star_%d' % d for d in range(1, 32, 1)]
    card_star_df = card_star_df[columns]
    # print card_star_df

    table = 'dis_card_star'
    print date, table
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, card_star_df, del_sql)

if __name__ == '__main__':
    for platform in ['superhero2', ]:
    #for platform in ['superhero2_tw', 'superhero2']:
        settings_dev.set_env(platform)
        for date in date_range('20180713', '20180715'):
            dis_card(date)