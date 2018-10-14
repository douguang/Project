#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 充值玩家数据 (UID，充值总额)（去水）
'''
from utils import hql_to_df, ds_add
import pandas as pd
import settings_dev
from pandas import DataFrame


settings_dev.set_env('dancer_tx_beta')
# 获取水军数据
df = pd.read_excel("/Users/kaiqigu/Downloads/Excel/water_uid.xlsx")
first_pay_sql = '''
SELECT user_id,
       sum(order_money) sum_money
FROM raw_paylog
WHERE ds >= '20160913'
GROUP BY user_id
'''
first_pay_df = hql_to_df(first_pay_sql)

first_pay_df['is_shui'] = first_pay_df['user_id'].isin(df.user_id.values)
result = first_pay_df[~first_pay_df['is_shui']]

result.to_excel('/Users/kaiqigu/Downloads/Excel/level_pay.xlsx')

