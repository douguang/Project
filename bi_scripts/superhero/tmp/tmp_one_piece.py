#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : pub藏宝图数据、限时兑换数据
'''
from utils import hql_to_df, ds_add
import pandas as pd
import settings
from pandas import DataFrame


settings.set_env('superhero_bi')
# one_piece.exchange
# omni_exchange.omni_exchange
sql = '''
SELECT uid,
       args
FROM raw_action_log
WHERE action = 'one_piece.exchange'
  AND substr(uid,1,1) ='g'
  AND ds = '20160817'
'''
tt_df = hql_to_df(sql)

uid_list,id_list = [],[]
for i in range(len(tt_df)):
    uid = tt_df.iloc[i,0]
    args = tt_df.iloc[i,1]
    args = eval(args)
    if args.has_key('id'):
        arg_id = args['id'][0]
        print arg_id
        uid_list.append(uid)
        id_list.append(arg_id)
    else:
        continue
data = DataFrame({'uid':uid_list,'id':id_list})
data['id'] = data['id'].map(lambda s: int(s))
id_list = [2876
,2877
,2878
,2846
,2847
,2848
,2816
,2817
,2818]
data['is_bao'] = data['id'].isin(id_list)
result = data[data['is_bao']]
del result['is_bao']

# 兑换人数
num_df = result.drop_duplicates(['id','uid'])
num_df = num_df.groupby('id').count().reset_index().rename(columns={'uid':'num'})

# 兑换次数
times_df = result.groupby('id').count().reset_index().rename(columns={'uid':'times'})

result_df = num_df.merge(times_df,on= 'id')

result_df.to_excel('/Users/kaiqigu/Downloads/Excel/one_piece.xlsx')




