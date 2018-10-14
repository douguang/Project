#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 修改info第一天的时间戳
Time        : 2017.03.17
'''
import settings_dev
import time
from utils import hql_to_df


settings_dev.set_env('jianniang_test')

sql = '''
select * from raw_info
'''
df = hql_to_df(sql)


df['reg_times'] = df['reg_time'].map(
    lambda s: time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(s))))
df['last_action_times'] = df['last_action_time'].map(
    lambda s: time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(s))))
result = df[[
    'now', 'user_id', 'coin', 'gold', 'hp_bottle', 'exp', 'water', 'food',
    'gas', 'medal', 'stone', 'fame', 'friendly', 'hp_train_pill',
    'atk_train_pill', 'def_train_pill', 'can_use_devote', 'teach',
    'teach_speedup', 'last_action_times', 'oil', 'uuid', 'account', 'platform',
    'head_icon', 'reg_times', 'faction_id', 'developed_most', 'session_id'
]]
result.to_csv('/Users/kaiqigu/Documents/h5_data/info', sep = '\t', index = False, header = False)

