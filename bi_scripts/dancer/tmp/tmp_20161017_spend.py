#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 限时兑换（去水）
'''
from utils import hql_to_df, ds_add
import pandas as pd
import settings_dev
from pandas import DataFrame


settings_dev.set_env('dancer_tw')
# 获取水军数据
df = pd.read_excel("/Users/kaiqigu/Downloads/Excel/tw_test_uid.xlsx")
login_sql = '''
SELECT DISTINCT user_id
FROM parse_actionlog
WHERE ds >= '20160907'
  AND ds <='20160913'
'''
login_df = hql_to_df(login_sql)
spend_sql = '''
select user_id,goods_type,sum(coin_num) sum_coin,count(user_id) uid_num
from raw_spendlog
where ds >='20160907'
and ds <='20161016'
group by user_id,goods_type
'''
spend_df = hql_to_df(spend_sql)

login_df['is_shui'] = login_df['user_id'].isin(df.user_id.values)
result = login_df[~login_df['is_shui']]

spend_df['is_act'] = spend_df['user_id'].isin(result.user_id.values)
spend_df_result = spend_df[spend_df['is_act']]

spend_df_result['is_act'] = 1
result_df = spend_df_result.groupby('goods_type').sum().reset_index()

result_df.to_excel('/Users/kaiqigu/Downloads/Excel/spend.xlsx')



