#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 豪华签到
'''
from utils import hql_to_df, ds_add
import pandas as pd
import settings_dev
from pandas import DataFrame


settings_dev.set_env('dancer_tw')

# 获取水军数据
df = pd.read_excel("/Users/kaiqigu/Downloads/Excel/tw_test_uid.xlsx")

award_sql ='''
SELECT user_id,
       min(log_t) log_t,
       max(vip) vip,
       count(user_id) uid_num
FROM parse_actionlog
WHERE ds >='20161001'
  AND ds <='20161017'
  AND a_typ = 'daily_award.coin_award'
GROUP BY user_id
'''
award_df = hql_to_df(award_sql)
pay_sql = '''
SELECT user_id,
       sum(order_money) sum_money,
       max(order_money) max_money
FROM raw_paylog
WHERE ds >='20161001'
  AND ds <='20161017'
GROUP BY user_id
'''
pay_df = hql_to_df(pay_sql)

# reg_sql = '''
# SELECT user_id,
#        min(log_t) reg_time
# FROM parse_actionlog
# WHERE ds >='20161001'
#   AND ds <='20161017'
# GROUP BY user_id
# '''
# reg_df = hql_to_df(reg_sql)

reg_sql = '''
SELECT user_id,
       min(reg_time) reg_time
FROM parse_info
WHERE ds >='20161001'
  AND ds <='20161017'
GROUP BY user_id
'''
reg_df = hql_to_df(reg_sql)

result = (award_df.merge(pay_df,on = 'user_id',how='left')
     .merge(reg_df,on = 'user_id',how='left'))

# 去除水军
result['is_shui'] = result['user_id'].isin(df.user_id.values)
result = result[~result['is_shui']]

columns = ['user_id','reg_time','vip','uid_num','sum_money','max_money']
result = result[columns]

result.to_excel('/Users/kaiqigu/Downloads/Excel/haohua.xlsx')

# 详细数据
sql = '''
SELECT user_id,
       log_t
FROM parse_actionlog
WHERE ds >='20161001'
  AND ds <='20161017'
  AND a_typ = 'daily_award.coin_award'
order BY user_id,log_t
'''
result_df = hql_to_df(sql)
# 去除水军
result_df['is_shui'] = result_df['user_id'].isin(df.user_id.values)
result_df = result_df[~result_df['is_shui']]

result_df.to_excel('/Users/kaiqigu/Downloads/Excel/haohua_mingxi.xlsx')

