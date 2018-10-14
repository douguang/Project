#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 美国版LTV数据
Create date : 2016.05.18
'''
import settings
from utils import hql_to_df, update_mysql, ds_add
from pandas import Series,DataFrame
from utils import get_config
import pandas as pd

def sum_money_info(date,num):
    sum_money_sql = '''
    select  sum(order_rmb) as sum_money
    from    raw_paylog
    left semi join
    (
        select uid from raw_reg where ds = '{date}'
    )   new_user_tbl
    on raw_paylog.user_id = new_user_tbl.uid
    where raw_paylog.ds >='{date}' and raw_paylog.ds <='{date_num}'
    '''.format(**{
        'date': date,
        'date_num':ds_add(date,num)
        })
    print sum_money_sql
    sum_money_df = hql_to_df(sum_money_sql)
    return sum_money_df

def result_info(sum_money_df,num,date,reg_uid_df):
    result = sum_money_df
    result['ds']=date
    result['r%s_ltv' %num] = result['sum_money'] / reg_uid_df['uid_num']
    result = sum_money_df
    columns = ['ds','r%s_ltv' %num]
    result = result[columns]
    return result

def result_data_info(date):
    reg_uid_sql = "select count(uid) as uid_num from raw_reg where ds = '{0}'".format(date)
    print reg_uid_sql
    reg_uid_df = hql_to_df(reg_uid_sql)
    sum_money_r3_df = sum_money_info(date,2)
    sum_money_r7_df = sum_money_info(date,6)
    sum_money_r14_df = sum_money_info(date,13)
    sum_money_r30_df = sum_money_info(date,29)
    result_r3 = result_info(sum_money_r3_df,3,date,reg_uid_df)
    result_r7 = result_info(sum_money_r7_df,7,date,reg_uid_df)
    result_r14 = result_info(sum_money_r14_df,14,date,reg_uid_df)
    result_r30 = result_info(sum_money_r30_df,30,date,reg_uid_df)
    result_data = result_r3.merge(result_r7,on=['ds'])
    result_data = result_data.merge(result_r14,on=['ds'])
    result_data = result_data.merge(result_r30,on=['ds'])
    return result_data

if __name__ == '__main__':
    settings.set_env('superhero_usa')
    print 'please wait a minuate'
    date = '20150919'
    result_data = result_data_info(date)
    for i in range(1,17):
        date = ds_add(date,1)
        print date
        result = result_data_info(date)
        result_data = pd.concat([result_data,result])
    print result_data

    result_data.to_excel('/Users/kaiqigu/Downloads/Excel/usa_ltv.xlsx')




