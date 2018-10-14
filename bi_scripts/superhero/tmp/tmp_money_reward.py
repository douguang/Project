#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 20160626赏金令
'''
import settings
from utils import hqls_to_dfs
import pandas as pd
from pandas import DataFrame

settings.set_env('superhero_bi')
# settings.set_env('superhero_qiku')
date = '20160626'

coin_sql = '''
SELECT *
FROM
  (SELECT CASE
              WHEN sum_coin>=1000
                   AND sum_coin <= 3000 THEN '1000-3000'
              WHEN sum_coin>3000
                   AND sum_coin <= 6000 THEN '3000-6000'
              WHEN sum_coin>6000 THEN '6000+'
          END AS coin_name ,
          user_id ,
          sum_coin
   FROM
     (SELECT user_id,
             sum(order_coin) AS sum_coin
      FROM raw_paylog
      WHERE ds = '20160626'
      GROUP BY user_id )a )b
WHERE coin_name IS NOT NULL
'''
reward_sql = '''
SELECT uid,
       args
FROM raw_action_log
WHERE action ='bounty_order.get_big_reward'
  AND ds = '20160626'
'''
coin_df, reward_df = hqls_to_dfs([coin_sql, reward_sql])
# coin_df['is_reward'] = coin_df['user_id'].isin(reward_df.uid.values)
# result.to_excel('/Users/kaiqigu/Downloads/Excel/reward.xlsx')
uid_list,args_list = [],[]
for i in range(len(reward_df)):
    uid = reward_df.iloc[i,0]
    args = reward_df.iloc[i,1]
    args = eval(args)
    print args
    if args.has_key('type'):
        print args['type']
        uid_list.append(uid)
        args_list.append(args['type'])
    else:
        continue
data = DataFrame({'user_id':uid_list,'args':args_list})
result = coin_df.merge(data,on='user_id',how='outer')
data.to_excel('/Users/kaiqigu/Downloads/Excel/reward.xlsx')




