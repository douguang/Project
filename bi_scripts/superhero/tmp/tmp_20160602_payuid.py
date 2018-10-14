#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 充值uid
'''
import pandas as pd
from pandas import DataFrame
from utils import date_range, hqls_to_dfs,ds_add,hql_to_df
import settings

settings.set_env('superhero_qiku')

monthpay_sql = '''
select '201601' ds ,user_id,sum(order_money) as sum_money from raw_paylog where ds like '201601%' group by '201601',user_id
union
select '201602' ds ,user_id,sum(order_money) as sum_money from raw_paylog where ds like '201602%' group by '201602',user_id
union
select '201603' ds ,user_id,sum(order_money) as sum_money from raw_paylog where ds like '201603%' group by '201603',user_id
union
select '201604' ds ,user_id,sum(order_money) as sum_money from raw_paylog where ds like '201604%' group by '201604',user_id
union
select '201605' ds ,user_id,sum(order_money) as sum_money from raw_paylog where ds like '201605%' group by '201605',user_id
'''
his_sql = '''
select user_id,sum(order_money) sum_hismoney from mid_paylog_all where  ds='20160531' group by user_id
'''
act_sql = "select distinct uid from raw_act where ds like '201605%'"

monthpay_df,his_df,act_df = hqls_to_dfs([monthpay_sql,his_sql,act_sql])

uid5000_df = monthpay_df[monthpay_df['sum_money'] >= 5000]
monthpay_df['is_5000_uid'] =monthpay_df['user_id'].isin(uid5000_df.user_id.values)
monthpay_df_result = monthpay_df[monthpay_df['is_5000_uid']]
del monthpay_df_result['is_5000_uid']

result = (monthpay_df_result.pivot_table('sum_money', ['user_id'], 'ds').reset_index().merge(his_df,on = 'user_id').reset_index())

result['is_act_uid'] = result['user_id'].isin(act_df.uid.values)
result = result.fillna(0)
columns = ['user_id','sum_hismoney','201601', '201602',   '201603',   '201604',   '201605','is_act_uid']
result =result[columns]

result.to_excel('/Users/kaiqigu/Downloads/Excel/qikuchongzhi5000.xlsx')
