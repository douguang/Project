#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 中间表综合数据,卡牌信息
Time        : 2017.04.07
'''
import settings_dev
import pandas as pd
from utils import hqls_to_dfs
from utils import get_config
from sqls_for_games.superhero import info_sql
from sqls_for_games.superhero import gs_sql
from sqls_for_games.superhero import card_sql
from sqls_for_games.superhero import super_sql


def get_card_name():
    character_detail_config = get_config('character_detail')
    occupation_config = get_config('occupation')
    # 用于判断神卡
    occupation_list = []
    for key in occupation_config.keys():
        if occupation_config[key] == 6:
            occupation_list.append(key)

    def card_lines():
        for card_id in character_detail_config.keys():
            yield [card_id, character_detail_config[card_id]['name'],
                   character_detail_config[card_id]['animation']]

    card_name_df = pd.DataFrame(card_lines(),
                                columns=['card_id', 'card_name', 'animation'])
    card_name_df = card_name_df[card_name_df['animation'].isin(
        occupation_list)]
    card_name_df = card_name_df.drop_duplicates('card_id')
    return card_name_df[['card_id', 'card_name']]


def mart_card(date):
    card_name_df = get_card_name()
    info_df, gs_df, card_df, super_df = hqls_to_dfs(
        [info_sql.format(date=date), gs_sql, card_sql.format(date=date),
         super_sql.format(date=date)])
    card_df['card_id'] = card_df['card_id'].fillna(0).astype('int')
    card_name_df['card_id'] = card_name_df['card_id'].fillna(0).astype('int')
    # 排除不需要的卡牌
    card_df = card_name_df.merge(card_df, on='card_id')
    card_result = (card_df.merge(info_df, on='user_id', how='left'))
    # 排除测试用户
    card_result = card_result[~card_result['user_id'].isin(
        gs_df.user_id.values)]
    # 加入超进化
    # card_result.merge(
    #     super_df, on=['user_id', 'card_id'], how='left').fillna(0)
    column = ['card_id', 'card_name', 'user_id', 'is_fight', 'jinjie',
              'zhuansheng', 'name', 'server', 'platform', 'plat', 'account',
              'level', 'vip']
    card_result = card_result[column]
    return card_result


if __name__ == '__main__':
    settings_dev.set_env('superhero_vt')
    date = '20180213'
    mart_card_df = mart_card(date)
    print mart_card_df
