#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 分国家、渠道（ios和android）LTV
Time        : 2017.03.21
'''
from utils import hqls_to_dfs, ds_add, hql_to_df, date_range
import settings_dev
import pandas as pd
from pandas import DataFrame
from ipip import IP

settings_dev.set_env('superhero_tw')
start_date = '20170112'
end_date = '20170115'
final_date = '20170420'
date = start_date
ltv_days = [7, 14, 30]

file_path = '/Users/kaiqigu/Documents/scripts/nginx_log/'
result = pd.read_table(file_path + 'log_account')
result = result.drop_duplicates('account')

IP.load(file_path + '/tinyipdata_utf8.dat')
result['ip'] = result['ip'].astype(basestring)


# result['country'] = result['ip'].map(lambda s: IP.find(s).strip().encode("utf8"))
def ip_lines():
    for _, row in result.iterrows():
        # print row
        ip = row.ip
        account = row.account
        country = IP.find(ip).strip().encode("utf8")
        if '中国台湾' in country:
            country = '台湾'
        elif '中国香港' in country:
            country = '香港'
        elif '中国澳门' in country:
            country = '澳门'
        yield [account, country]


result = pd.DataFrame(ip_lines(), columns=['account', 'country'])

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
        platform_2 = row.platform_2
        if platform_2 == 'iosfacebook':
            platform_2 = 'ios'
        elif platform_2 == 'ioskvgames':
            platform_2 = 'ios'
        else:
            platform_2 = 'android'
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

# result_df.to_excel('/Users/kaiqigu/Documents/Excel/ip_ltv.xlsx')

result_df.to_csv('/Users/kaiqigu/Documents/Excel/ip_ltv',
                 sep='\t', index=False, header=False)
