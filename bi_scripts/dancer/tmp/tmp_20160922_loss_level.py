#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 自营留存
'''
from utils import hqls_to_dfs, ds_add, update_mysql,date_range
import settings_dev
from pandas import DataFrame
import numpy as np
import pandas as pd

settings_dev.set_env('dancer_ks_beta')
# date = '20160913'
# date1= ds_add(date,1)
# date2 = ds_add(date,2)

df = pd.read_excel("/Users/kaiqigu/Downloads/Excel/water_uid.xlsx")

def loss_rate(date, date1, date2):
    # info_sql = '''
    # SELECT ds,
    #        user_id
    # FROM parse_info
    # WHERE ds IN ('{date1}',
    #              '{date2}')
    # '''.format(date1 = date1,date2 = date2)
    info_sql = '''
    SELECT DISTINCT ds,
                    user_id
    FROM mid_actionlog
    WHERE ds IN ('{date1}',
                 '{date2}')
    '''.format(date1 = date1,date2 = date2)
    # reg_sql = '''
    # SELECT user_id,
    #        level
    # FROM parse_info
    # WHERE ds ='{date}'
    #   AND regexp_replace(substr(reg_time ,1,10),'-','') ='{date}'
    # '''.format(date = date)
    reg_sql = '''
    WITH aa AS
      (SELECT user_id,
              min(log_t) reg_time
       FROM mid_actionlog
       WHERE ds >='20160913'
         AND ds<='20160920'
       GROUP BY user_id) ,
         bb AS
      (SELECT user_id
       FROM aa
       WHERE regexp_replace(substr(reg_time ,1,10),'-','') ='{date}' )
    SELECT user_id,
           max(LEVEL) LEVEL
    FROM mid_actionlog
    WHERE ds = '{date}'
      AND user_id IN
        (SELECT user_id
         FROM bb)
    GROUP BY user_id
    '''.format(date = date)
    reg_df,info_df = hqls_to_dfs([reg_sql,info_sql])

    reg_df['is_shui'] = reg_df['user_id'].isin(df.user_id.values)
    reg_result_df = reg_df[~reg_df['is_shui']]

    info_df['is_shui'] = info_df['user_id'].isin(df.user_id.values)
    info_result_df = info_df[~info_df['is_shui']]

    info1_df = info_result_df.loc[info_result_df.ds == date1]
    info2_df = info_result_df.loc[info_result_df.ds == date2]

    reg_df['is_d1'] = reg_df['user_id'].isin(info1_df.user_id.values)
    reg_df['is_d2'] = reg_df['user_id'].isin(info2_df.user_id.values)

    reg1_df = reg_df[~reg_df['is_d1']]
    reg2_df = reg_df[~reg_df['is_d2']]

    # 注册用户数
    reg_data = reg_df.groupby('level').count().user_id.reset_index()

    # 次流数据
    d2_result_data = reg1_df.groupby('level').count().user_id.reset_index().rename(columns = {'user_id':'d2_keep'})

    # 3流数据
    d3_result_data = reg2_df.groupby('level').count().user_id.reset_index().rename(columns = {'user_id':'d3_keep'})

    result = (reg_data.merge(d2_result_data,on='level',how='outer')
                      .merge(d3_result_data,on='level',how='outer'))
    result['ds'] = date
# # result = reg_data.merge(d2_result_data,on=['platform_2','addr'],how='outer')

    return result

if __name__ == '__main__':
    date_list = date_range('20160919','20160920')
    dfs = []
    for date in date_list:
        date1 = ds_add(date,1)
        date2 = ds_add(date,2)
        print date
        data = loss_rate(date,date1,date2)
        dfs.append(data)
    result_df = pd.concat(dfs)
    result_df = result_df.fillna(0)

result_df.to_excel('/Users/kaiqigu/Downloads/Excel/loss_rate.xlsx')
