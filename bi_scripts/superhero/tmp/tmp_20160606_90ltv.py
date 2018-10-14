#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 90日LTV
3日LTV：    当日新增人数在3日内的充值总金额/当日新增的人数
'''
import settings
from utils import hqls_to_dfs, update_mysql, ds_add
from pandas import DataFrame
import pandas as pd

def r90_ltv_info(reg_df,pay_df,start_date):
    reg_result = reg_df[reg_df['ds'] == start_date]
    pay_result = pay_df[(pay_df['ds'] >= start_date) & (pay_df['ds'] <= ds_add(start_date,90-1))]
    pay_result['is_reg'] = pay_result['user_id'].isin(reg_result.uid.values)
    pay_result = pay_result[pay_result['is_reg']]
    reg_num = reg_result.count().uid
    pay_money = pay_result.sum().order_money
    pay_rate = pay_money/reg_num
    data = {'ds':[start_date],'reg_num':[reg_num],'pay_money':[pay_money],'r90_ltv':[pay_rate]}
    frame = DataFrame(data)

    return frame

if __name__ == '__main__':

    settings.set_env('superhero_bi')
    start_date = '20151223'
    end_date = '20160301'

    # # ltv日期
    # pay_days = [90]

    reg_sql = "select ds,uid from raw_reg where substr(uid,1,1) ='a' and ds >= '{0}' and ds <= '{1}'".format(start_date,end_date)
    pay_sql = "select ds,user_id,order_money from raw_paylog where substr(user_id,1,1) ='a' and ds >= '{0}' and ds <= '{1}'".format(start_date,ds_add(end_date,90))

    print reg_sql
    print pay_sql
    reg_df,pay_df = hqls_to_dfs([reg_sql,pay_sql])

    date = start_date
    data_result = r90_ltv_info(reg_df,pay_df,date)
    for i in range(100):
        date = ds_add(date,1)
        print date
        data = r90_ltv_info(reg_df,pay_df,date)
        if date != end_date:
            data_result = pd.concat([data_result,data])
        else:
            break
    print data_result


data_result.to_excel('/Users/kaiqigu/Downloads/Excel/90ltv.xlsx')



