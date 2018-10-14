#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 2017/10/30 0030 14:07
@Author  : Andy 
@File    : reg_android-ios_ltv_on_account.py
@Software: PyCharm
Description :
'''

from ipip import *
from utils import date_range, ds_add, hql_to_df
import settings_dev
import pandas as pd

def data_reduce(start_ds,end_ds):
    # 每天每个语言注册人数
    reg_sql='''
        select regexp_replace(substr(reg_time,1,10),'-','') as reg_ds,account,user_id from mid_info_all where ds='{end_ds}' group by reg_ds,account,user_id
    '''.format(start_ds=start_ds,end_ds=end_ds)
    print reg_sql
    reg_df = hql_to_df(reg_sql)
    print reg_df.head()


    # 设备
    equipment_sql = '''
         select user_token as user_id,case when identifier  like '%appstore%' then 'apple' else 'android' end as language
         from parse_nginx
         where ds >= '20170701'
         and ds <= '{end_ds}'
         group by user_token,case when identifier  like '%appstore%' then 'apple' else 'android' end
       '''.format(start_ds=start_ds, end_ds=end_ds)
    print equipment_sql
    equipment_df = hql_to_df(equipment_sql)
    print equipment_df

    reg_df = reg_df.merge(equipment_df, on=['user_id', ], how='left')
    reg_df['language'] = reg_df.language.fillna('android')
    reg_df = reg_df.drop_duplicates(subset=['account', ], keep='first')
    print reg_df.head()

    # pay
    pay_sql = '''
        select ds,user_id,sum(order_money) as pay_rmb from raw_paylog where ds>='{start_ds}' and ds<='{end_ds}' and platform_2 != 'admin_test' group by ds,user_id
    '''.format(start_ds=start_ds,end_ds=end_ds)
    print pay_sql
    pay_df = hql_to_df(pay_sql)
    pay_df = pay_df.merge(reg_df, on=['user_id', ], how='left')
    print pay_df.head()
    # 按account统计收入
    pay_df = pay_df.groupby(['ds', 'account','reg_ds','language',]).agg({
        'pay_rmb': lambda g: g.sum(),
    }).reset_index()
    print pay_df.head()

    # 统计每种语言的注册人数  account
    reg_mid = reg_df.groupby(['reg_ds', 'language',]).agg({
        'account': lambda g: g.nunique(),
    }).reset_index().rename(columns={'account': 'reg_num', 'reg_ds': 'ds',})
    print reg_mid.head()

    # 统计LTV
    dates = date_range(start_ds, end_ds)
    ltvday = 30
    ltv_days = range(1, ltvday + 1)
    reg_df = reg_df[['account', 'language', 'reg_ds',]]
    reg_df = reg_df.sort_values(['account', 'language', 'reg_ds',], ascending=True)
    reg_df = reg_df.drop_duplicates(subset=['account', 'language',], keep='first')

    reg_ltv_df = reg_df[['account','reg_ds','language']].drop_duplicates().rename(columns={'reg_ds': 'ds'})
    daily_ltv_dfs = []
    for date in dates:
        reg_daily_df = reg_ltv_df.loc[reg_ltv_df.ds == date].copy()
        for ltv_day in ltv_days:
            ltv_date = ds_add(date, ltv_day - 1)
            ltv_range_pay_df = pay_df.loc[(pay_df.ds >= date) & (pay_df.ds <= ltv_date)][
                ['account', 'pay_rmb']].groupby('account').sum().reset_index()
            reg_daily_df = (reg_daily_df
                            .merge(ltv_range_pay_df, on='account', how='left')
                            .rename(columns={'pay_rmb': 'd%d_pay_rmb' % ltv_day})
                            .fillna(0)
                            )
        reg_daily_df['new_num'] = 1
        reg_daily_df = reg_daily_df.groupby(['ds', 'language']).sum()

        daily_ltv_dfs.append(reg_daily_df)

    result_df = pd.concat(daily_ltv_dfs).reset_index()
    for ltv_day in ltv_days:
        result_df['d%d_ltv' % ltv_day] = result_df[
                                             'd%d_pay_rmb' % ltv_day] / result_df.new_num

    columns = ['ds', 'language', 'new_num'] + ['d%d_pay_rmb' %
                                              ltv_day for ltv_day in ltv_days] + ['d%d_ltv' % ltv_day for ltv_day in
                                                                                  ltv_days]
    df = result_df[columns]
    print pd.DataFrame(df).head()

    return df

if __name__ == '__main__':
    date = '20171001'
    end_date = '20171120'
    settings_dev.set_env('sanguo_tl')
    result_df = data_reduce(date,end_date)
    result_df.to_excel('E:\sanguo_tl_android-ios_ltv_on_account-20171121.xlsx')
    print 'end'