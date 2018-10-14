#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description :  武娘台服 12月各国家每日新增和次留 三留 七留 14留 30留数据 按照account统计，分安卓和IOS，韩鹏需求。(方法太笨了，以后不用了)
Database    : dancer_tw
'''
import settings_dev
import pandas as pd
from utils import hql_to_df, ds_add, date_range
from ipip import *

def tmp_20170112_country_loss(date):

    #判断新增
    reg_sql = '''
    select t1.ds, t1.account, t1.regist_ip, t2.platform from (
        select ds, account, regist_ip from parse_info where ds='{date}' and regexp_replace(to_date(reg_time),'-','')='{date}'
        and account not in (select account from mid_info_all where ds='{date_before}')) t1 left join (
        select account, platform from parse_actionlog where ds='{date}' group by account, platform
        ) t2 on t1.account = t2.account
    '''.format(date=date, date_before=ds_add(date, -1))
    print reg_sql
    reg_df = hql_to_df(reg_sql).fillna(0)
    print reg_df.head(10)
    reg_df['regist_ip'] = reg_df['regist_ip'].astype(basestring)
    IP.load(os.path.abspath("./tinyipdata_utf8.dat"))

    def ip_lines():
        for _, row in reg_df.iterrows():
            ip = row.regist_ip
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
                yield [row.ds, row.account, country, row.platform]
            except:
                pass

    reg_df = pd.DataFrame(ip_lines(), columns=['ds', 'account', 'country', 'platform'])
    reg_sum_df = reg_df.groupby(['ds', 'country', 'platform']).agg({
        'account': lambda g: g.count()
    }).reset_index().rename(columns={
        'account': 'reg_num'
    })
    print reg_sum_df.head(10)

    #判断次日留存
    liucun_sql_2 = '''
        select account from parse_info where ds='{date_after}'
    '''.format(date_after=ds_add(date, 1))
    print liucun_sql_2
    liucun_df_2 = hql_to_df(liucun_sql_2)
    print liucun_df_2.head(10)

    liucun_sum_df_2 = reg_df[reg_df['account'].isin(liucun_df_2['account'])]
    liucun_sum_df_2 = liucun_sum_df_2.groupby(['ds', 'country', 'platform']).agg({
        'account': lambda g:g.count(),
    }).reset_index().rename(columns={
        'account': 'liucun_2'
    })
    print liucun_sum_df_2.head(10)

    result_df = reg_sum_df.merge(liucun_sum_df_2, on=['ds', 'country', 'platform'], how='left').fillna(0)
    print result_df.head(10)

    # 判断3日留存
    liucun_sql_3 = '''
            select account from parse_info where ds='{date_after}'
        '''.format(date_after=ds_add(date, 2))
    print liucun_sql_3
    liucun_df_3 = hql_to_df(liucun_sql_3)
    print liucun_df_3.head(10)

    liucun_sum_df_3 = reg_df[reg_df['account'].isin(liucun_df_3['account'])]
    liucun_sum_df_3 = liucun_sum_df_3.groupby(['ds', 'country', 'platform']).agg({
        'account': lambda g: g.count(),
    }).reset_index().rename(columns={
        'account': 'liucun_3'
    })
    print liucun_sum_df_3.head(10)

    result_df = result_df.merge(liucun_sum_df_3, on=['ds', 'country', 'platform'], how='left').fillna(0)
    print result_df.head(10)

    # 判断7日留存
    liucun_sql_7 = '''
            select account from parse_info where ds='{date_after}'
        '''.format(date_after=ds_add(date, 6))
    print liucun_sql_7
    liucun_df_7 = hql_to_df(liucun_sql_7)
    print liucun_df_7.head(10)

    liucun_sum_df_7 = reg_df[reg_df['account'].isin(liucun_df_7['account'])]
    liucun_sum_df_7 = liucun_sum_df_7.groupby(['ds', 'country', 'platform']).agg({
        'account': lambda g: g.count(),
    }).reset_index().rename(columns={
        'account': 'liucun_7'
    })
    print liucun_sum_df_7.head(10)

    result_df = result_df.merge(liucun_sum_df_7, on=['ds', 'country', 'platform'], how='left').fillna(0)
    print result_df.head(10)

    # 判断14日留存
    liucun_sql_14 = '''
            select account from parse_info where ds='{date_after}'
        '''.format(date_after=ds_add(date, 13))
    print liucun_sql_14
    liucun_df_14 = hql_to_df(liucun_sql_14)
    print liucun_df_14.head(10)

    liucun_sum_df_14 = reg_df[reg_df['account'].isin(liucun_df_14['account'])]
    liucun_sum_df_14 = liucun_sum_df_14.groupby(['ds', 'country', 'platform']).agg({
        'account': lambda g: g.count(),
    }).reset_index().rename(columns={
        'account': 'liucun_14'
    })
    print liucun_sum_df_14.head(10)

    result_df = result_df.merge(liucun_sum_df_14, on=['ds', 'country', 'platform'], how='left').fillna(0)
    print result_df.head(10)

    # 判断30日留存
    if ds_add(date, 29) not in date_range('20170119', '20170131'):
        liucun_sql_30 = '''
                select account from parse_info where ds='{date_after}'
            '''.format(date_after=ds_add(date, 29))
        print liucun_sql_30
        liucun_df_30 = hql_to_df(liucun_sql_30)
        print liucun_df_30.head(10)

        liucun_sum_df_30 = reg_df[reg_df['account'].isin(liucun_df_30['account'])]
        liucun_sum_df_30 = liucun_sum_df_30.groupby(['ds', 'country', 'platform']).agg({
            'account': lambda g: g.count(),
        }).reset_index().rename(columns={
            'account': 'liucun_30'
        })
        print liucun_sum_df_30.head(10)

        result_df = result_df.merge(liucun_sum_df_30, on=['ds', 'country', 'platform'], how='left').fillna(0)
        print result_df.head(10)

        columns = ['ds', 'country', 'platform', 'reg_num', 'liucun_2', 'liucun_3', 'liucun_7', 'liucun_14', 'liucun_30']
        result_df = result_df[columns]

    return result_df



if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    result_list = []
    for date in date_range('20161126', '20161130'):
        result_list.append(tmp_20170112_country_loss(date))
    result = pd.concat(result_list)
    result.to_excel('/home/kaiqigu/Documents/hanpeng1.xlsx')
