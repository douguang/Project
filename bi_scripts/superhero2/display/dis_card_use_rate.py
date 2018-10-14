#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 卡牌使用率
Database    : superhero2
'''
import settings_dev
import pandas as pd
from utils import hql_to_df, update_mysql, get_config, date_range


def dis_card_use_rate(date):


    hero_sql = '''
        select t1.user_id, t1.server, t2.vip, t1.hero_id, t1.rn from (
        select reverse(substr(reverse(user_id), 8)) as server, user_id, hero_id, row_number() over(partition by user_id order by combat desc) as rn from parse_hero where ds='{date}') t1
        left join (
        select user_id, vip from parse_info where ds='{date}') t2 on t1.user_id = t2.user_id
    '''.format(**{'date': date})
    print hero_sql
    card_df = hql_to_df(hero_sql)

    info_config = get_config('hero_basis')
    # 拥有人数
    total_df = card_df.groupby(['server', 'vip', 'hero_id']).user_id.count().reset_index().rename(columns={'user_id': 'total_num'})
    # 使用人数
    user_df = card_df[card_df['rn'] <= 5]
    rn_dic = {num: 'rn_%d'%num for num in range(1, 6, 1)}
    user_rate_df = pd.pivot_table(user_df, values='user_id', index=['server', 'vip', 'hero_id'], columns='rn', aggfunc='count', fill_value=0).reset_index().rename(columns=rn_dic)
    result_df = total_df.merge(user_rate_df, on=['server', 'vip', 'hero_id'], how='left').fillna(0)
    for i in range(1, 6, 1):
        result_df['use_rate_%d'%i] = result_df['rn_%d'%i]*1.0/result_df['total_num']
    result_df['hero_name'] = result_df['hero_id'].map(lambda x: info_config[str(x)]['name'])
    result_df['ds'] = date
    # print result_df
    columns = ['ds', 'server', 'vip', 'hero_id', 'hero_name', 'total_num', 'rn_1', 'use_rate_1', 'rn_2', 'use_rate_2', 'rn_3', 'use_rate_3', 'rn_4', 'use_rate_4', 'rn_5', 'use_rate_5']
    result_df = result_df[columns]
    print result_df.head(10)

    #更新MySQL
    table = 'dis_card_use_rate'
    print date, table
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)

if __name__ == '__main__':
    for platform in ['superhero2']:
        settings_dev.set_env(platform)
        for date in date_range('20170512', '20170522'):
            dis_card_use_rate(date)