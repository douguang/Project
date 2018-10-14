#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 2017/8/24 0024 20:09
@Author  : Andy 
@File    : reg_country_ltv_on_account_ltv.py
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
        select regexp_replace(substr(reg_time,1,10),'-','') as reg_ds,ip as language,account,user_id from mid_info_all where ds='{end_ds}' group by reg_ds,language,account,user_id
    '''.format(start_ds=start_ds,end_ds=end_ds)
    print reg_sql
    reg_df = hql_to_df(reg_sql)
    # rep_dic = {None: '泰语', '0': '英文', '1': '简中', '2': '繁中', '3': '泰语', '4': '越南语', '5': '印尼语'}
    # reg_df['language'] = reg_df.replace(rep_dic).language
    print reg_df.head()
    # reg_df.to_excel(r'/home/kaiqigu/桌面/1.xlsx', index=False)
    reg_df['language'] = reg_df['language'].astype(basestring)
    IP.load(os.path.abspath("./tinyipdata_utf8.dat"))

    def ip_lines():
        for _, row in reg_df.iterrows():
            ip = row.language
            try:
                country = IP.find(ip).strip().encode("utf8")
                if '中国台湾' in country:
                    country = '台湾'
                elif '中国香港' in country:
                    country = '香港'
                elif '中国澳门' in country:
                    country = '澳门'
                elif '中国' in country:
                    country = '中国'
                yield [row.user_id, country]
            except:
                pass

    country_df = pd.DataFrame(ip_lines(), columns=['user_id', 'language'])
    print country_df.head()

    reg_df = reg_df[['reg_ds','account','user_id',]]
    reg_df = reg_df.merge(country_df, on=['user_id', ], how='left')
    print reg_df

    reg_df = reg_df.drop_duplicates(subset=['account', ], keep='first')



    # reg_df = reg_df.drop_duplicates(subset=['account', 'language',], keep='last')
    # reg_df.to_excel(r'/home/kaiqigu/桌面/2.xlsx', index=False)
    print reg_df.head()
    print '-----------'
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
    # reg_df.to_excel(r'/home/kaiqigu/桌面/4.xlsx', index=False)
    print "************"
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
    # df.to_excel(r'/home/kaiqigu/桌面/3.xlsx', index=False)


    return df

if __name__ == '__main__':
    date = '20170901'
    end_date = '20171008'
    settings_dev.set_env('sanguo_tl')
    result_df = data_reduce(date,end_date)
    # result_df = pd.DataFrame(result_df)
    result_df.to_excel('E:\sanguo_tl_ltv_on_account-20171009.xlsx')
    print 'end'