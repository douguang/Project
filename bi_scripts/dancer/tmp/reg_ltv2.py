#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
from utils import hqls_to_dfs, ds_add, update_mysql, hql_to_df,date_range
import settings_dev
import pandas as pd
from pandas import DataFrame

settings_dev.set_env('dancer_ks_beta')

df = pd.read_excel("/Users/kaiqigu/Downloads/Excel/water_uid.xlsx")

def d3_ltv(start_date,end_date):
    pay_sql = '''
    SELECT a.ds,
           b.user_id,
           b.platform,
           a.order_money
    FROM
      (SELECT ds,
              user_id,
              order_money
       FROM raw_paylog
       WHERE ds >= '{start_date}'
       and ds <= '{end_date}')a
    JOIN
      (SELECT substr(account,1,instr(account,'_')-1) platform,
              user_id
       FROM parse_info
       WHERE ds ='{start_date}'
         AND regexp_replace(substr(reg_time,1,10),'-','') = '{start_date}')b
         ON a.user_id = b.user_id
    '''.format(start_date = start_date,end_date = end_date)
    reg_sql = '''
    SELECT regexp_replace(substr(reg_time,1,10),'-','') ds,
           substr(account,1,instr(account,'_')-1) platform,
           user_id
    FROM parse_info
    WHERE ds ='{start_date}'
      AND regexp_replace(substr(reg_time,1,10),'-','') = '{start_date}'
      group by regexp_replace(substr(reg_time,1,10),'-',''),platform,user_id
    '''.format(start_date = start_date,end_date = end_date)
    pay_df,reg_df = hqls_to_dfs([pay_sql,reg_sql])

    pay_df['is_shui'] = pay_df['user_id'].isin(df.user_id.values)
    reg_df['is_shui'] = reg_df['user_id'].isin(df.user_id.values)
    pay_df = pay_df[~pay_df['is_shui']]
    reg_df = reg_df[~reg_df['is_shui']]

    reg_data = (reg_df.groupby(['ds','platform'])
                    .count()
                    .user_id
                    .reset_index()
                    .rename(columns = {'user_id':'reg_num'}))
    pay_data = (pay_df.groupby(['platform'])
                    .sum()
                    .order_money
                    .reset_index()
                    .rename(columns = {'order_money':'sum_money'}))
    result = reg_data.merge(pay_data,on = 'platform',how = 'outer')
    result['d3_ltv'] = result['sum_money']*1.0/result['reg_num']

    return result

# date_list = date_range(start_date,end_date)
if __name__ == '__main__':
    date_list = date_range('20160913','20160917')
    dfs = []
    for start_date in date_list:
        end_date = ds_add(start_date,2)
        print start_date
        data = d3_ltv(start_date,end_date)
        dfs.append(data)
    result_df = pd.concat(dfs)
    result_df = result_df.fillna(0)

result_df.to_excel('/Users/kaiqigu/Downloads/Excel/d3_ltv.xlsx')

