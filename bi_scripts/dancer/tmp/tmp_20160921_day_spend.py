#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 每日消费数据（去水）
'''
from utils import hql_to_df, ds_add
import pandas as pd
import settings_dev
from pandas import DataFrame


settings_dev.set_env('dancer_tx_beta')
# 获取水军数据
df = pd.read_excel("/Users/kaiqigu/Downloads/Excel/water_uid.xlsx")
# 每日消费数据
first_pay_sql = '''
SELECT ds,
       reverse(substr(reverse(user_id),8)) AS server,
       user_id,
       goods_type,
       coin_num
FROM raw_spendlog
WHERE ds>= '20160913'
'''
first_pay_df = hql_to_df(first_pay_sql)

# 去除水军
first_pay_df['is_shui'] = first_pay_df['user_id'].isin(df.user_id.values)
result = first_pay_df[~first_pay_df['is_shui']]

# 金额
money_df = result.groupby(['ds','server','goods_type']).sum().coin_num.reset_index()

money_df.to_excel('/Users/kaiqigu/Downloads/Excel/day_spend.xlsx')

