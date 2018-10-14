#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 武娘多语言 - 购买和消耗体力
Time        : 2017.07.14
illustration: 购买体力：shop.shop_buy  shop_id=1001
消耗体力：Dmp:Energy 行为日志中
'''
import settings_dev
import pandas as pd
from utils import hqls_to_dfs
from utils import hql_to_df

settings_dev.set_env('dancer_mul')
spend_sql = '''
SELECT ds,
       user_id,
       goods_type,
       args
FROM raw_spendlog
WHERE goods_type = 'shop.shop_buy'
'''
info_sql = '''
SELECT ds,
       user_id,
       vip
FROM parse_info
'''
spend_df, info_df = hqls_to_dfs([spend_sql, info_sql])


def get_value():
    for _, row in spend_df.iterrows():
        yield [row.ds, row.user_id, int(eval(row.args).get('shop_id', [0])[0])]


column = ['ds', 'user_id', 'shop_id']
result = pd.DataFrame(get_value(), columns=column)
result_df = result[result.shop_id == 1001]
result_df = result_df.merge(info_df, on=['ds', 'user_id'])
# 购买体力的人数和次数
result_df = result_df.groupby(['ds', 'vip']).agg({
    'user_id': 'nunique',
    'shop_id': 'count',
}).reset_index()
result_df.to_excel('/Users/kaiqigu/Documents/Excel/shop_buy.xlsx')

# 体力消耗
action_sql = '''
SELECT ds,user_id,
       vip,
       a_rst
FROM parse_actionlog
WHERE ds = '20170709'
  AND a_rst LIKE '%Dmp:Energy%'
'''
action_df = hql_to_df(action_sql)


def get_energy():
    for _, row in action_df.iterrows():
        for i in eval(row.a_rst):
            if i['obj'].strip() == 'Dmp:Energy':
                ti = int(i['diff'])
                yield [row.ds, row.user_id, row.vip, ti]


column = ['ds', 'user_id', 'vip', 'ti']
result = pd.DataFrame(get_energy(), columns=column)
result_df = result.groupby(['ds', 'vip']).ti.sum().reset_index()
result_df.to_excel('/Users/kaiqigu/Documents/Excel/spend_ti.xlsx')
