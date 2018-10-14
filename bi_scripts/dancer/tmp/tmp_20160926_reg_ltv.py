#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
from utils import ds_add, hql_to_df,date_range
import settings_dev
import pandas as pd

settings_dev.set_env('dancer_tw')

df = pd.read_excel("/Users/kaiqigu/Downloads/Excel/water_uid.xlsx")
country_df = pd.read_excel("/Users/kaiqigu/Downloads/Excel/country.xlsx")

reg_sql = '''
SELECT user_id,
       regexp_replace(substr(reg_time ,1,10),'-','') reg_time
FROM
  (SELECT user_id,
          min(ds) ds
   FROM mid_actionlog
   WHERE ds<='20161010'
   GROUP BY user_id )a
'''
reg_df = hql_to_df(reg_sql)

guid_df = guid_df.drop_duplicates(['user_id'])

reg_df['is_shui'] = reg_df['user_id'].isin(df.user_id.values)
reg_df = reg_df[~reg_df['is_shui']]
reg_df = reg_df.merge(country_df,on='user_id',how='left')

def d3_ltv(start_date,end_date):
    pay_sql = '''
    SELECT user_id,
           sum(order_money) sum_money
    FROM raw_paylog
    WHERE ds >= '{start_date}'
      AND ds <= '{end_date}'
    GROUP BY user_id
    '''.format(start_date = start_date,end_date = end_date)
    pay_df = hql_to_df(pay_sql)

    reg_data = reg_df.loc[reg_df.ds == start_date]
    reg_data = reg_data.merge(guid_df,on='user_id',how='left').fillna(0)

    pay_df['is_shui'] = pay_df['user_id'].isin(df.user_id.values)
    pay_df = pay_df[~pay_df['is_shui']]

    pay_result_df = pay_df.merge(reg_data,on = ['user_id','is_shui'])


    reg_data_df = (reg_data.groupby(['ds','platform','country'])
                    .count()
                    .user_id
                    .reset_index()
                    .rename(columns = {'user_id':'reg_num'}))
    pay_data_df = (pay_result_df.groupby(['ds','platform','country'])
                    .sum()
                    .sum_money
                    .reset_index())
    result = reg_data_df.merge(pay_data_df,on = ['ds','platform','country'],how = 'outer')
    result['d3_ltv'] = result['sum_money']*1.0/result['reg_num']

    return result

if __name__ == '__main__':
    date_list = date_range('20160915','20160922')
    dfs = []
    for start_date in date_list:
        end_date = ds_add(start_date,6)
        print start_date
        data = d3_ltv(start_date,end_date)
        dfs.append(data)
    result_df = pd.concat(dfs)
    result_df = result_df.fillna(0)

result_df = result_df.rename(columns={'reg_num':'reg_num_7','sum_money':'sum_money_7','d3_ltv':'d7_ltv'})
result = d3_data.merge(result_df,on = ['ds','platform','country'],how='outer')

result_df.to_excel('/Users/kaiqigu/Downloads/Excel/d7_ltv.xlsx')
