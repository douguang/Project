#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 通天钱庄 - 相当于超级英雄的福利基金
Time        : 2017.06.19
illustration: 日期  服  ID 激活基金的人数
'''
import pandas as pd
import settings_dev
from utils import hql_to_df

settings_dev.set_env('dancer_mul')
sql = '''
SELECT ds,
       user_id,
       server,
       a_typ,
       a_tar args
FROM parse_actionlog
WHERE a_typ = 'server_welfare_fund.activate'
'''
tt_df = hql_to_df(sql)


def get_id():
    for _, row in tt_df.iterrows():
        yield [row.ds, row.user_id, row.server, eval(row.args)['id']]


column = ['ds', 'user_id', 'server', 'id']
result_df = (pd.DataFrame(get_id(), columns=column).groupby(
    ['ds', 'server', 'id']).count().reset_index())
result_df['id'] = result_df['id'].astype('int')
result_df.to_excel('/Users/kaiqigu/Documents/Excel/welfare_fund.xlsx')
