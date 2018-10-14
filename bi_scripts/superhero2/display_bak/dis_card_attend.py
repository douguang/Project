#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 上阵的卡牌数据(加工为文本文件)
加工后的文件存储路径：/home/data/superhero2/redis_stats/card_attend_{0}
备注：
战斗前请求接口， 上阵的英雄在参数里面参数名为team
1. 关卡      private_city.battle_data
2. 战争工厂   daily_boss.battle_data
3. 无尽梦靥   daily_nightmares.battle_data
4. 血尘拉力赛 rally.battle_data
5. 竞技场    未定
'''
from utils import hql_to_df, update_mysql
import settings
import pandas as pd
from pandas import DataFrame

def dis_card_attend(date):
    card_sql = '''
    SELECT user_id,
           post_level,
           action,
           args
    FROM parse_action_log
    WHERE action IN ('private_city.battle_data',
                     'daily_boss.battle_data',
                     'daily_nightmares.battle_data',
                     'rally.battle_data')
      AND ds ='{0}'
    '''.format(date)
    card_df = hql_to_df(card_sql)

    dfs = []
    for _,row in card_df.iterrows():
        msg =  eval(row['args'])
        if msg.has_key('team'):
            card_msg = msg['team'][0].split('_')
            for i in card_msg:
                data = DataFrame({'user_id':[row['user_id']],'level':[row['post_level']],'card_id':[i.split('-')[0]]})
                dfs.append(data)
    card_attend_df = pd.concat(dfs)

    columns = ['user_id','level','card_id']
    card_attend_df = card_attend_df[columns]

    card_attend_df.to_csv('/home/data/superhero2/redis_stats/card_attend_{0}'.format(date), sep = '\t', index = False, header = False)
    print 'card_attend_{0} file complete'.format(date)

if __name__ == '__main__':
    settings.set_env('superhero2')
    date = '20160816'
    dis_card_attend(date)

