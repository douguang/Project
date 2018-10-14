#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 龙脉宝藏
Time        : 2017.07.10
illustration: 类别  次数  人数
'''
import settings_dev
import pandas as pd
from utils import hql_to_df

settings_dev.set_env('dancer_pub')
bowl_sql = '''
SELECT user_id,
       a_tar as args
FROM parse_actionlog
WHERE ds='20170708'
  AND a_typ = 'bowl.choice'
'''
df = hql_to_df(bowl_sql)


def get_value():
    for _, row in df.iterrows():
        yield [row.user_id, eval(row.args)['num'], eval(row.args)['sort']]


column = ['user_id', 'num', 'sort']
bowl_df = pd.DataFrame(get_value(), columns=column)
result_df = bowl_df.groupby('sort').agg({
    'num': 'sum',
    'user_id': 'nunique',
}).reset_index().rename(columns={'num': 'times',
                                 'user_id': 'user_num'})
result_df.to_excel('/Users/kaiqigu/Documents/Excel/bowl.xlsx')
