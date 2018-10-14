#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 卡牌概况
'''
from utils import hql_to_df, update_mysql
import settings
from utils import get_config
import pandas as pd

def dis_card_profile(date):
    card_star_list = range(1,6)
    card_star_dic = {num: 'd%d_num' % num for num in card_star_list}
    ranges = ranges = range(-1,100,10)
    ranges[0] = 0
    ranges.append(100)
    columns_to_rename = {
        '(0, 9]': '1_9',
        '(9, 19]': '10_19',
        '(19, 29]': '20_29',
        '(29, 39]': '30_39',
        '(39, 49]': '40_49',
        '(49, 59]': '50_59',
        '(59, 69]': '60_69',
        '(69, 79]': '70_79',
        '(79, 89]': '80_89',
        '(89, 99]': '90_99',
        '(99, 100]': '99_100',
    }

    card_sql = '''
    SELECT a.user_id,
           a.card_id,
           a.card_star_level,
           a.card_level,
           CASE
               WHEN level >=1
                    AND level <= 9 THEN '1_9'
               WHEN level >=10
                    AND level <= 19 THEN '10_19'
               WHEN level >=20
                    AND level <= 29 THEN '20_29'
               WHEN level >=30
                    AND level <= 39 THEN '30_39'
               WHEN level >=40
                    AND level <= 49 THEN '40_49'
               WHEN level >=50
                    AND level <= 59 THEN '50_59'
               WHEN level >=60
                    AND level <= 69 THEN '60_69'
               WHEN level >=70
                    AND level <= 79 THEN '70_79'
               WHEN level >=80
                    AND level <= 89 THEN '80_89'
               WHEN level >=90
                    AND level <= 99 THEN '90_99'
               WHEN level >=100
                    AND level <= 100 THEN '100'
               ELSE 'None'
           END level_ivtl
    FROM
      (SELECT user_id,
              card_id,
              card_star_level,
              card_level
       FROM parse_hero
       WHERE ds = '{date}' )a
    JOIN
      ( SELECT user_id,
               level
       FROM parse_info
       WHERE ds='{date}' )b ON a.user_id = b.user_id
    '''.format(date=date)
    card_name_sql = '''
    SELECT card_id,
           card_name
    FROM cfg_character_detail
    '''
    card_name_df = hql_to_df(card_name_sql)
    card_df = hql_to_df(card_sql)
    card_df['num'] = 1
    # 卡牌数量
    card_num_df = (card_df.groupby(['card_star_level','level_ivtl','card_id'])
                   .count().reset_index()
                   .loc[:, ['card_id','card_star_level','level_ivtl','num']]
                    .rename(columns={'num': 'card_num'}))
    # 卡牌等级
    card_level_df = (card_num_df
             .groupby(['level_ivtl','card_id',pd.cut(card_num_df.card_star_level, ranges)])
             .sum().card_num.reset_index()
             .pivot_table('card_num', ['level_ivtl','card_id'], 'card_star_level')
             .reset_index()
             .rename(columns=columns_to_rename))
    # 星级卡牌
    card_star_df = (card_num_df
        .pivot_table('card_num', ['level_ivtl','card_id'], 'card_star_level')
        .reset_index()
        .rename(columns=card_star_dic))

    for i in ['d%d_num' % num for num in card_star_list]:
        if i not in card_star_df.columns:
            card_star_df[i] = 0

    result_df = (card_star_df
      .merge(card_level_df,on=['level_ivtl','card_id'],how='outer')
      .merge(card_num_df,on=['level_ivtl','card_id'],how='outer'))

    # 卡牌名
    result_df = result_df.merge(card_name_df, on='card_id', how='left')
    # result_df = card_name_df.merge(result_df, on='card_id', how='left')
    result_df = result_df.fillna(0)
    result_df['ds'] = date

    columns = ['ds','card_id','card_name','level_ivtl','card_num'] + ['d%d_num' % num for num in card_star_list] + ['1_9','10_19','20_29','30_39','40_49','50_59','60_69','70_79','80_89','90_99','99_100']
    result_df = result_df[columns]
    print result_df

    # 更新MySQL表
    table = 'dis_card_profile_superhero'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)

if __name__ == '__main__':
    settings.set_env('superhero2')
    date = '20160719'
    dis_card_profile(date)
