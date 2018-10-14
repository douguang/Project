#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 每日充值人数
and substr(uid,1,1) = 'g'
'''
import settings
from utils import hqls_to_dfs, update_mysql, ds_add,hql_to_df
import pandas as pd

def dis_sup_spend_details(date):
    old_date = pd.to_datetime(ds_add(date, -29))
    # 判断新老服
    server_type_sql = '''
    SELECT reverse(substr(reverse(uid), 8)) AS server,
           to_date(min(create_time)) AS server_date
    FROM mid_info_all
    WHERE ds = '{date}'
      AND length(create_time) = 19
      AND to_date(create_time) != '1970-01-01'
    GROUP BY reverse(substr(reverse(uid), 8))
    '''.format(**{
        'date': date,
    })
    server_type_df = hql_to_df(server_type_sql)
    server_type_df['server_date'] = pd.to_datetime(server_type_df.server_date)
    server_type_df['type'] = server_type_df.server_date.map(lambda x: 'new_server' if x >= old_date else 'old_server')
    return server_type_df

if __name__ =='__main__':
    settings.set_env('superhero_bi')
    pay_sql = '''
    SELECT ds,
           uid,
           reverse(substr(reverse(uid), 8)) AS server,
           order_money
    FROM raw_paylog
    WHERE ds>='20160401'
      AND ds<='20160630'
      and substr(uid,1,1) = 'a'
    '''
    pay_df = hql_to_df(pay_sql)
    # 判断新老服
    server_type_sql = '''
    SELECT reverse(substr(reverse(uid), 8)) AS server,
           to_date(min(create_time)) AS server_date
    FROM mid_info_all
    WHERE ds = '20160630'
      AND length(create_time) = 19
      AND to_date(create_time) != '1970-01-01'
    GROUP BY reverse(substr(reverse(uid), 8))
    '''
    server_type_df = hql_to_df(server_type_sql)
    server_df = server_type_df
    date = '20160401'
    while date <= '20160630':
        today = pd.Period('%s' % date)
        server_type_df['%s' % date] = server_type_df.server_date.map(lambda ds: today - pd.Period(ds))
        new_data = server_type_df[(server_type_df['%s' % date]<=29) & (server_type_df['%s' % date] >= 0)]
        new_data['%s' % date] = 'new'
        old_data = server_type_df[server_type_df['%s' % date]>29]
        old_data['%s' % date] = 'old'
        other_data = server_type_df[server_type_df['%s' % date] < 0]
        other_data['%s' % date] = 'other'
        server_type_df = pd.concat([new_data,old_data,other_data])
        print date
        date = ds_add(date,1)
    # pay_df = pay_df.drop_duplicates(['ds','uid','server'])
    # 每日充值金额
    pay_df = pay_df.groupby(['ds','uid','server']).sum().reset_index()
    result = pay_df.merge(server_type_df,on='server',how='left')
    # 每日充值总人数
    pay_num = result.groupby('ds').count().reset_index().loc[:,['ds','uid']]
    # 新老服充值人数
    date = '20160401'
    data = result[result['ds'] == '20160401']
    new_num_df = data.groupby(['ds','20160401']).count().reset_index().loc[:,['ds','20160401','uid']].rename(columns={'20160401':'server'})
    new_money_df = data.groupby(['ds','20160401']).sum().reset_index().loc[:,['ds','20160401','order_money']].rename(columns={'20160401':'server'})
    print date
    date = ds_add(date,1)
    while date <= '20160630':
        data = result[result['ds'] == '%s' % date]
        new_num = data.groupby(['ds','%s' % date]).count().reset_index().loc[:,['ds','%s' % date,'uid']].rename(columns={'%s' % date:'server'})
        new_money = data.groupby(['ds','20160401']).sum().reset_index().loc[:,['ds','20160401','order_money']].rename(columns={'20160401':'server'})
        new_num_df = pd.concat([new_num_df,new_num])
        new_money_df = pd.concat([new_money_df,new_money])
        print date
        date = ds_add(date,1)
    new_pay_data = new_num_df.pivot_table('uid', ['ds'], 'server').reset_index()
    new_money_data = new_money_df.pivot_table('order_money', ['ds'], 'server').reset_index()
    new_money_data = new_money_data.rename(columns={'new':'new_money','old':'old_money'})
    result_df = pay_num.merge(new_pay_data,on='ds').merge(new_money_data,on='ds')

    columns =['ds','uid','old','new','new_money']
    result_df = result_df[columns]
    result_df.to_excel('/Users/kaiqigu/Downloads/Excel/chongzhi.xlsx')






