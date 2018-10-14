#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : vip等级礼包（去水）
shop_id对应的数字减一，表示对应等级的VIP礼包
'''
from utils import hql_to_df, ds_add
import pandas as pd
import settings_dev
from pandas import DataFrame

settings_dev.set_env('dancer_tw')
# 获取水军数据
df = pd.read_excel("/Users/kaiqigu/Downloads/Excel/water_uid.xlsx")
first_pay_sql = '''
SELECT ds,
       user_id,
       subtime,
       args
FROM raw_spendlog
WHERE ds = '20160926'
  AND goods_type = 'shop.vip_buy'
'''
first_pay_df = hql_to_df(first_pay_sql)

first_pay_df['is_shui'] = first_pay_df['user_id'].isin(df.user_id.values)
result = first_pay_df[~first_pay_df['is_shui']]

dfs = []
for _, row in result.iterrows():
    args = eval(row['args'])
    if args.has_key('shop_id'):
        data = DataFrame({'ds':row['ds'],'user_id':row['user_id'],'subtime':row['subtime'],'id':[args['shop_id'][0]]})
        dfs.append(data)

result_df = pd.concat(dfs)

# vip礼包12、14、15
aa = result_df.loc[result_df.id == '13']
bb = result_df.loc[result_df.id == '15']
cc = result_df.loc[result_df.id == '16']
result = pd.concat([aa,bb,cc])

first_pay_df.to_excel('/Users/kaiqigu/Downloads/Excel/vip_gift.xlsx')
