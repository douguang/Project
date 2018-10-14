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

settings_dev.set_env('dancer_tx_beta')

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
       WHERE ds >='{start_date}'
         AND ds < ='{end_date}'
         AND regexp_replace(substr(reg_time,1,10),'-','') >= '{start_date}'
         AND regexp_replace(substr(reg_time,1,10),'-','') <= '{end_date}')b
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
    date_list = date_range('20160913','20160920')
    dfs = []
    for start_date in date_list:
        end_date = ds_add(start_date,2)
        print start_date
        data = d3_ltv(start_date,end_date)
        dfs.append(data)
    result_df = pd.concat(dfs)
    result_df = result_df.fillna(0)

result_df.to_excel('/Users/kaiqigu/Downloads/Excel/d3_ltv.xlsx')


# dfs = []
# for date in date_list:
#     print date
#     reg_data = reg_df.loc[reg_df.ds == date]
#     dt = reg_data.drop_duplicates(['ds','platform','user_id'])
#     # reg_num = reg_data.user_id.nunique()
#     data = reg_data.groupby('platform').count().user_id.reset_index().rename(columns={'user_id':'reg_user_num'})
#     # data = {}
#     data['ds'] = date
#     # data['reg_user_num'] = [reg_num]
#     for i in ltv_day:
#         ltv_days = [ds_add(date,dt) for dt in range(0,i)]
#         pay_df['is_use'] = pay_df['ds'].isin(ltv_days)
#         result = pay_df[pay_df['is_use']]
#         result['is_reg'] = result['user_id'].isin(reg_data.user_id.values)
#         result = result[result['is_reg']]
#         ltv_end_date = ds_add(date, i - 1)
#         if ltv_end_date > final_date:
#             data['d%d_pay_num' %i] = 0
#             data['d%d_ltv' %i] = 0
#         else:
#             result_data = result.drop_duplicates(['ds','user_id','platform'])
#             # data['d%d_pay_num' %i] = result.user_id.nunique()
#             pay_user_num = result_data.groupby('platform').count().user_id.reset_index().rename(columns={'user_id':'d%d_pay_num' %i})
#             ltv_num = result.groupby('platform').sum().order_money.reset_index()
#             data = (data
#                         .merge(pay_user_num,on = 'platform',how = 'left')
#                         .merge(ltv_num,on = 'platform',how = 'left')
#                         )
#             data['d%d_ltv' %i] = data.order_money*1.0/data['reg_user_num']

#     result_df = DataFrame(data)
#     columns = ['ds','reg_user_num'] + ['d%d_pay_num' %i for i in ltv_day] + ['d%d_ltv' %i for i in ltv_day]
#     result_df = result_df[columns]
#     dfs.append(result_df)
# df = pd.concat(dfs)
# print df

# df.to_excel('/Users/kaiqigu/Downloads/Excel/lz_ltv.xlsx')
