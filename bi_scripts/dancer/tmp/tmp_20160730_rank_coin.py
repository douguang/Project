#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 3日活跃人数汇总表
Name        : dis_d3_act_user_num
'''
import settings_dev
from utils import hql_to_df, update_mysql, ds_add, date_range, hqls_to_dfs
from pandas import DataFrame
import pandas as pd
def rank_coin():
    combat_sql = '''
    SELECT user_id,
           combat
    FROM mid_info_all
    WHERE ds = '20160815'
      AND user_id IN
        (SELECT user_id
         FROM mid_actionlog
         WHERE ds = '20160808')
    ORDER BY combat DESC LIMIT 20
    '''
    level_sql = '''
    SELECT user_id,
           level
    FROM mid_info_all
    WHERE ds = '20160815'
      AND user_id IN
        (SELECT user_id
         FROM mid_actionlog
         WHERE ds = '20160808')
    ORDER BY level DESC LIMIT 20
    '''
    cost_sql = '''
    SELECT user_id,
           goods_type,
           sum(coin_num) AS cost
    FROM raw_spendlog
    WHERE ds >= '20160808'
      AND ds <= '20160815'
    GROUP BY user_id,
             goods_type
    ORDER BY user_id,
             sum(coin_num) DESC'''
    combat_df, level_df, cost_df = hqls_to_dfs([combat_sql, level_sql, cost_sql])
    combat_coin_df = cost_df.merge(combat_df,on='user_id',how='inner')
    level_coin_df = cost_df.merge(level_df,on='user_id',how='inner')
    writer = pd.ExcelWriter('/Users/kaiqigu/Documents/dancer/tmp_20160819_rank_coin.xlsx')
    combat_coin_df.to_excel(writer,'combat_coin')
    level_coin_df.to_excel(writer,'level_coin')
if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    rank_coin()
