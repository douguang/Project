#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 审计 - 道具销售
'''
from utils import hql_to_df
import settings
import numpy as np
import pandas as pd

settings.set_env('superhero_bi')
# 开始日期
date1 = '20160401'
# 截止日期
date2 = '20160630'
df = pd.read_excel("/Users/kaiqigu/Downloads/Excel/shenji/shenji_daoju.xlsx")

sale_sql = '''
SELECT goods_type,
          sum(coin_num) sum_coin
   FROM raw_spendlog
   WHERE ds>='{date1}'
    AND ds<='{date2}'
    AND goods_type in
    ('gacha.do_reward_gacha',
    'shop.buy',
    'magic_school.open_contract',
    'roulette.open_roulette10',
    'one_piece.open_roulette10'
    )
   GROUP BY goods_type
'''.format(date1=date1,date2=date2)
sale_df = hql_to_df(sale_sql)
result = pd.concat([sale_df,df])
result = result.groupby('goods_type').sum().reset_index()

result.to_excel('/Users/kaiqigu/Downloads/Excel/shenji/审计-道具销售.xlsx')
