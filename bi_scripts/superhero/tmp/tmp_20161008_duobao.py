#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
10月4日夺宝奇兵
轮盘单抽 ：raiders.open_raiders_roulette
轮盘十连抽：raiders.open_raiders_roulette_10
'''
from utils import hql_to_df
import settings
from pandas import DataFrame

settings.set_env('superhero_bi')

sql = '''
SELECT *
FROM raw_action_log
WHERE ds ='20161004'
  AND substr(uid,1,1) ='g'
  AND action = 'raiders.open_raiders_roulette_10'
'''
df = hql_to_df(sql)
