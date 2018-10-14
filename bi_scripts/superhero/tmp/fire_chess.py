#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 火焰棋盘数据
Date        : 2017-02-06
fire_chess.choice_reward 这是购买一次的接口
fire_chess.all_choice_reward 这是打包带走的接口 参数tp: tp =1 是购买10次 tp=2 是剩余的全部购买
all_choice_reward 这个接口不好测出购买几次
column：玩家ID	角色名	排名	积分	单次购买总共消耗的钻石	打包购买总共消耗的钻石	刷新总共消耗的钻石	购买60钻箱子消耗的钻石	购买70钻箱子消耗的钻石	购买80钻箱子消耗的钻石	购买90钻箱子消耗的钻石	购买100钻箱子消耗的钻石
'''
from utils import hqls_to_dfs, get_rank, hql_to_df
import pandas as pd
import settings_dev
from pandas import DataFrame

settings_dev.set_env('superhero_vt')
date = '20170423'

sql = '''
SELECT uid,
       goods_type,
       sum(coin_num) sum_coin
FROM raw_spendlog
WHERE ds ='{date}'
  AND goods_type IN ('fire_chess.choice_reward',
                     'fire_chess.refresh_shop',
                     'fire_chess.all_choice_reward')
GROUP BY uid,
         goods_type
'''.format(date=date)
coin_sql = '''
SELECT uid,
       coin_num,
       sum(coin_num) sum_coin
FROM raw_spendlog
WHERE ds ='{date}'
  AND goods_type = 'fire_chess.choice_reward'
GROUP BY uid,
         coin_num
'''.format(date=date)
info_sql = '''
select uid,nick from raw_info where ds ='{date}'
'''.format(date=date)
info_df = hql_to_df(info_sql)
jifen_df, coin_df = hqls_to_dfs([sql, coin_sql])

jifen_result = jifen_df.pivot_table(
    'sum_coin', ['uid'], 'goods_type').reset_index().fillna(0)
coin_result = coin_df.pivot_table(
    'sum_coin', ['uid'], 'coin_num').reset_index().fillna(0)
jifen_result['sum_coin'] = jifen_result['fire_chess.choice_reward'] + \
    jifen_result['fire_chess.refresh_shop'] + \
    jifen_result['fire_chess.all_choice_reward']
# jifen_result_df = get_rank(jifen_result, 'sum_coin', 100)
jifen_result_df = jifen_result.sort_values(by='sum_coin', ascending=False)
jifen_result_df['rank'] = range(1, (len(jifen_result_df) + 1))
result_df = jifen_result_df.merge(coin_result, on='uid', how='left').fillna(0)
result_df = result_df.merge(info_df, on='uid')
columns = [
    'uid', 'nick', 'rank', 'sum_coin', 'fire_chess.choice_reward', 'fire_chess.all_choice_reward', 'fire_chess.refresh_shop', 60, 70, 80, 90, 100
]
result_df = result_df[columns]

# result_df.to_excel('/Users/kaiqigu/Documents/Excel/fire_chess.xlsx')
result_df.to_csv('/Users/kaiqigu/Documents/Excel/fire_chess',
                 sep='\t', index=False)
