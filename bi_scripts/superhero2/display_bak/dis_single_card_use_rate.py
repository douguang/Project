#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 单卡使用率（并保存为文件）
备注：
生成数据：单卡使用率的数据
生成文件位置：/home/data/superhero2/redis_stats/card_use_rate_xxxxxxxx
'''
from utils import hqls_to_dfs, update_mysql
import settings
import pandas as pd
from pandas import DataFrame

def dis_single_card_use_rate(date):
    ranges = ranges = range(-1,100,10)
    ranges[0] = 0
    ranges.append(100)
    card_sql = '''
    SELECT user_id,
           level,
           card_id
    FROM parse_hero_attend
    WHERE ds ='{0}'
    '''.format(date)
    card_have_sql ='''
    SELECT a.user_id,
           a.card_id,
           b.level
    FROM
      (SELECT user_id,
              card_id
       FROM parse_hero
       WHERE ds = '{date}')a
    JOIN
      ( SELECT user_id,
               level
       FROM parse_info
       WHERE ds = '{date}')b ON a.user_id = b.user_id
    '''.format(date=date)
    card_name_sql = '''
    SELECT card_id,
           card_name
    FROM cfg_character_detail
    '''
    card_df,card_have_df,card_name_df = hqls_to_dfs([card_sql,card_have_sql,card_name_sql])

    # 卡牌使用人数
    card_use_df = card_df.drop_duplicates(['card_id','level','user_id'])
    card_use_num_df = (card_use_df
        .groupby(['card_id','level'])
        .count()
        .reset_index()
        .rename(columns={'user_id':'attend_num'}))
    # 分等级卡牌使用人数
    card_use_result_df = (card_use_num_df
        .groupby(['card_id',pd.cut(card_use_num_df.level, ranges)])
        .sum().attend_num.reset_index())
    # 卡牌拥有人数
    card_have_num_df = (card_have_df
        .groupby(['card_id','level'])
        .count()
        .reset_index()
        .rename(columns={'user_id':'have_user_num'}))
    # 分等级卡牌拥有人数
    card_have_result_df = (card_have_num_df
        .groupby(['card_id',pd.cut(card_have_num_df.level, ranges)])
        .sum().have_user_num.reset_index())

    card_use_result_df['card_id'] = card_use_result_df['card_id'].map(lambda s: int(s))
    result_df = (card_have_result_df
        .merge(card_use_result_df,on=['card_id','level'],how='left'))

    # 卡牌使用率
    result_df['use_rate'] = result_df['attend_num']*1.0/result_df['have_user_num']
    result_df = result_df.fillna(0)
    result_df['ds'] = date

    # 卡牌名
    result_df = result_df.merge(card_name_df, on='card_id', how='left')
    # result_df = card_name_df.merge(result_df, on='card_id', how='left')
    result_df = result_df.rename(columns={'level': 'level_ivtl'})

    columns = ['ds', 'level_ivtl', 'card_id', 'card_name', 'have_user_num','attend_num','use_rate']
    result_df = result_df[columns]
    print result_df

    # 更新MySQL表
    table = 'dis_single_card_use_rate'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)

    result_df.to_csv('/home/data/superhero2/redis_stats/card_use_rate_{0}'.format(date), sep = '\t', index = False, header = False)
    print 'card_use_rate_{0} file complete'.format(date)

if __name__ == '__main__':
    settings.set_env('superhero2')
    date = '20160816'
    dis_single_card_use_rate(date)
