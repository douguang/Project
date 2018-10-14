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

start_date = '20160913'
end_date = '20160918'
final_date = '20160918'
date = start_date
ltv_day = [3,7,14,30,60]

pay_sql = '''

'''.format(date=date,end_date=end_date)
reg_sql = '''
SELECT regexp_replace(substr(reg_time,1,10),'-','') ds,
       substr(account,1,instr(account,'_')-1) platform,
       user_id
FROM parse_info
WHERE ds >='20160913'
  AND ds < ='20160918'
  AND regexp_replace(substr(reg_time,1,10),'-','') >= '20160913'
  AND regexp_replace(substr(reg_time,1,10),'-','') <= '20160918'
  group by regexp_replace(substr(reg_time,1,10),'-',''),platform,user_id
'''.format(date,end_date)
pay_df,reg_df = hqls_to_dfs([pay_sql,reg_sql])

pay_df['is_shui'] = pay_df['user_id'].isin(df.user_id.values)
reg_df['is_shui'] = reg_df['user_id'].isin(df.user_id.values)
pay_df = pay_df[~pay_df['is_shui']]
reg_df = reg_df[~reg_df['is_shui']]

date_list = date_range(start_date,end_date)

dfs = []
for date in date_list:
    print date
    reg_data = reg_df.loc[reg_df.ds == date]
    dt = reg_data.drop_duplicates(['ds','platform','user_id'])
    # reg_num = reg_data.user_id.nunique()
    data = reg_data.groupby('platform').count().user_id.reset_index().rename(columns={'user_id':'reg_user_num'})
    # data = {}
    data['ds'] = date
    # data['reg_user_num'] = [reg_num]
    for i in ltv_day:
        ltv_days = [ds_add(date,dt) for dt in range(0,i)]
        pay_df['is_use'] = pay_df['ds'].isin(ltv_days)
        result = pay_df[pay_df['is_use']]
        result['is_reg'] = result['user_id'].isin(reg_data.user_id.values)
        result = result[result['is_reg']]
        ltv_end_date = ds_add(date, i - 1)
        if ltv_end_date > final_date:
            data['d%d_pay_num' %i] = 0
            data['d%d_ltv' %i] = 0
        else:
            result_data = result.drop_duplicates(['ds','user_id','platform'])
            # data['d%d_pay_num' %i] = result.user_id.nunique()
            pay_user_num = result_data.groupby('platform').count().user_id.reset_index().rename(columns={'user_id':'d%d_pay_num' %i})
            ltv_num = result.groupby('platform').sum().order_money.reset_index()
            data = (data
                        .merge(pay_user_num,on = 'platform',how = 'left')
                        .merge(ltv_num,on = 'platform',how = 'left')
                        )
            data['d%d_ltv' %i] = data.order_money*1.0/data['reg_user_num']

    result_df = DataFrame(data)
    columns = ['ds','reg_user_num'] + ['d%d_pay_num' %i for i in ltv_day] + ['d%d_ltv' %i for i in ltv_day]
    result_df = result_df[columns]
    dfs.append(result_df)
df = pd.concat(dfs)
print df

# df.to_excel('/Users/kaiqigu/Downloads/Excel/lz_ltv.xlsx')
