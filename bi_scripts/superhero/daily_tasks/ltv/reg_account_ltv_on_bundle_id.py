#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  分包LTV
@software: PyCharm 
@file: reg_account_ltv_on_bundle_id.py 
@time: 17/12/13 下午5:07 
"""
from utils import hqls_to_dfs, ds_add, hql_to_df, date_range
import settings_dev
import pandas as pd
from pandas import DataFrame
from ipip import IP


def data_reduce(start_date,end_date,final_date):

    date = start_date
    ltv_days = [1,2,3,4,5,6,7, 14, 30]

    result_sql = '''
        select account,bundle_id as country from mid_info_all where ds='{final_date}' group by account,bundle_id
    '''.format(final_date=final_date,)
    result = hql_to_df(result_sql)
    result = result.drop_duplicates('account')

    pay_sql = '''
    SELECT a.ds,
           b.account,
           sum(a.order_money) order_money
    FROM
      (SELECT ds,
              uid,
              order_money order_money
       FROM raw_paylog
       WHERE ds >= '{date}'
         AND platform_2 <> 'admin_test')a
    JOIN
      (SELECT DISTINCT uid,
                       account
       FROM mid_new_account
       WHERE ds >= '{date}'
         AND ds <= '{end_date}')b ON a.uid = b.uid
    GROUP BY a.ds,
             b.account
    '''.format(date=date, end_date=end_date)
    reg_sql = '''
    SELECT DISTINCT ds,
                    platform_2,
                    account
    FROM mid_new_account
    WHERE ds >= '{0}'
      AND ds <= '{1}'
    '''.format(date, end_date)
    pay_df, reg_df = hqls_to_dfs([pay_sql, reg_sql])


    def plat_lines():
        for _, row in reg_df.iterrows():
            ds = row.ds
            account = row.account
            platform_2 = 'platform'
            # platform_2 = row.platform_2
            # if platform_2 == 'iosfacebook':
            #     platform_2 = 'ios'
            # elif platform_2 == 'ioskvgames':
            #     platform_2 = 'ios'
            # else:
            #     platform_2 = 'android'
            yield [ds, account, platform_2]


    reg_df = pd.DataFrame(plat_lines(), columns=['ds', 'account', 'platform_2'])
    reg_df = reg_df.merge(result, on='account')
    # df.groupby(['ds','country']).count().reset_index()
    # date_list = date_range(start_date, end_date)
    # user_result = reg_df.merge(pay_df, on=['ds', 'account'])

    dfs = []
    # date = '20170112'
    for date in date_range(start_date, end_date):
        reg_data = reg_df[reg_df.ds == date]
        col_df = (reg_data.groupby(
            ['platform_2', 'country']).count().account.reset_index().rename(
                columns={'account': 'reg_num'}))
        col_df['ds'] = date
        for ltv_day in ltv_days:
            ltv_end_date = ds_add(date, ltv_day - 1)
            if ltv_end_date > final_date:
                col_df['d%s_money' % ltv_day] = 0
                col_df['d%s_ltv' % ltv_day] = 0
            else:
                pay_result_df = (pay_df[(pay_df.ds >= date) & (pay_df.ds <= ds_add(
                    date, ltv_day - 1))][['account', 'order_money']].groupby(
                        'account').sum().reset_index())
                pay_result_df = pay_result_df.merge(reg_data, on='account')
                pay_result_df = (pay_result_df.groupby(
                    ['platform_2', 'country']).sum().order_money.reset_index())
                col_df = (col_df.merge(pay_result_df,
                                       on=['platform_2', 'country'],
                                       how='left').fillna(0)
                          .rename(columns={'order_money': 'd%s_money' % ltv_day}))
                col_df['d%s_ltv' % ltv_day] = col_df['d%s_money' %
                                                     ltv_day] * 1.0 / col_df['reg_num']

        columns = ['ds', 'platform_2', 'country', 'reg_num'] + ['d%d_ltv' %
                                                                ltv_day for ltv_day in ltv_days] + ['d%d_money' % ltv_day for ltv_day in ltv_days]
        col_df = col_df[columns]
        dfs.append(col_df)

    result_df = pd.concat(dfs)


    return result_df


if __name__ == '__main__':
    platform = 'superhero_bi'
    settings_dev.set_env(platform)
    start_date = '20171208'
    end_date = '20171212'
    final_date = '20171212'
    res = data_reduce(start_date,end_date,final_date)
    res.to_excel('/Users/kaiqigu/Documents/Superhero/%s-分包LTV_20171213.xlsx' % platform)