#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 等级付费分布（去水）
'''
from utils import hql_to_df, ds_add
import pandas as pd
import settings_dev
from pandas import DataFrame


settings_dev.set_env('dancer_tx_beta')
# 获取水军数据
df = pd.read_excel("/Users/kaiqigu/Downloads/Excel/water_uid.xlsx")
first_pay_sql = '''
SELECT level,
       user_id,
       order_money
FROM raw_paylog
WHERE ds >= '20160913'
ORDER BY level
'''
first_pay_df = hql_to_df(first_pay_sql)

first_pay_df['is_shui'] = first_pay_df['user_id'].isin(df.user_id.values)
result = first_pay_df[~first_pay_df['is_shui']]

# 人数
user = result.drop_duplicates(['level','user_id'])
user_df = user.groupby('level').count().user_id.reset_index()
# 金额
money_df = result.groupby('level').sum().order_money.reset_index()

result_df = user_df.merge(money_df,on='level')
result_df.to_excel('/Users/kaiqigu/Downloads/Excel/level_pay.xlsx')

