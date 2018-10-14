#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 首次充值及对应等级（去水）
'''
from utils import hql_to_df, ds_add
import pandas as pd
import settings_dev
from pandas import DataFrame


settings_dev.set_env('dancer_tw')
# 获取水军数据
df = pd.read_excel("/Users/kaiqigu/Downloads/Excel/tw_test_uid.xlsx")
first_pay_sql = '''
SELECT ds,
       order_time,
       user_id,
       platform_2
FROM raw_paylog
WHERE ds >= '20160907'
  AND ds <='20160926'
  AND order_time <='2016-09-26 15:48:00'
  AND product_id = '11'
  AND platform_2 = 'apple'
ORDER BY ds,
         order_time
'''
first_pay_df = hql_to_df(first_pay_sql)

first_pay_df['is_shui'] = first_pay_df['user_id'].isin(df.user_id.values)
result = first_pay_df[~first_pay_df['is_shui']]

result.to_excel('/Users/kaiqigu/Downloads/Excel/growth_fund.xlsx')

