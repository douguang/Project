#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# 三国和武娘的综合数据表脚本
# 计算>>>dau，新增用户，当日收入，arpu，arppu，活跃vip用户，新增vip用户，当日新增收入，次日留存
from utils import hql_to_df, update_mysql
import pandas as pd
import settings_dev
import numpy as np
from pandas import DataFrame
import os


def daily_report(date, db):
    settings_dev.set_env(db)
    sql = '''
    SELECT ds,
           user_id,
           reg_time,
           act_time,
           today_pay,
           all_pay,
           vip
    FROM mart_assist
    WHERE ds = '{date}'
    '''.format(date=date)
    df = hql_to_df(sql)
    df['today'] = pd.to_datetime(date)
    df['act_time'] = df.act_time.astype('datetime64')
    df['reg_time'] = df.reg_time.astype('datetime64')
    # dau
    dau_df = DataFrame(df[['ds', 'user_id']][df['act_time'] == df['today']], columns=[
                       'ds', 'user_id']).groupby('ds').count().reset_index()
    # 新增用户
    nau_df = DataFrame(df[['ds', 'user_id']][df['reg_time'] == df['today']], columns=[
                       'ds', 'user_id']).groupby('ds').count().reset_index()
    # 当日收入
    pay_df = df.groupby('ds').sum().today_pay.reset_index()
    # 当日付费用户
    dpu_df = DataFrame(df[['ds', 'user_id']][df['today_pay'] > 0], columns=[
                       'ds', 'user_id']).groupby('ds').count().reset_index()
    # 活跃VIP
    vip_df = DataFrame(df[['ds', 'user_id']][(df['act_time'] == df['today']) & (
        df['vip'] >= 1)], columns=['ds', 'user_id']).groupby('ds').count().reset_index()
    # 新增VIP
    nvu_df = DataFrame(df[['ds', 'user_id']][(df['today_pay'] == df['all_pay']) & (
        df['vip'] >= 1)], columns=['ds', 'user_id']).groupby('ds').count().reset_index()
    # 新增收入
    npi_df = DataFrame(df[['ds', 'today_pay']][df['today_pay'] == df['all_pay']], columns=[
                       'ds', 'today_pay']).groupby('ds').sum().reset_index()
    # 次日留存用户
    mku_df = DataFrame(df[['ds', 'user_id']][((df['act_time'] - df['reg_time']) / np.timedelta64(1, 'D')) == 1], columns=[
                       'ds', 'user_id']).groupby('ds').count().reset_index()
    # 合并汇总
    result = dau_df.merge(
        nau_df, on='ds', how='inner').merge(
        pay_df, on='ds', how='inner').merge(
        vip_df, on='ds', how='inner').merge(
        nvu_df, on='ds', how='inner').merge(
        npi_df, on='ds', how='inner').merge(
        mku_df, on='ds', how='inner').merge(
        dpu_df, on='ds', how='inner').reset_index()
    result.columns = ['index', 'ds', 'dau', 'nau',
                      'today_income', 'act_vip', 'new_vip', 'new_income', 'keep_user', 'pay_user']
    result['ARPU'] = (result.today_income / result.dau).round(2)
    result['ARPPU'] = (result.today_income / result.pay_user).round(2)
    result['pay_rate'] = (result.pay_user / result.dau).round(2)
    result['db'] = db
    if 'tw' in db:
        result['today_income'] = (result['today_income'] / 5.0).round(0)
        result['new_income'] = (result['new_income'] / 5.0).round(0)
    elif 'kr' in db:
        result['today_income'] = (result['today_income'] / 5.0).round(0)
        result['new_income'] = (result['new_income'] / 5.0).round(0)
    elif 'tl' in db:
        result['today_income'] = (result['today_income'] / 3200.0).round(0)
        result['new_income'] = (result['new_income'] / 3200.0).round(0)
    return result
    # table = 'dis_user_report_detail'
    # del_sql = 'delete from {0} where ds="{1}" and db="{2}"'.format(
    #     table, date, db)
    # update_mysql(table, result, del_sql, 'report')

if __name__ == '__main__':
    date = '20170320'
    dblist = ['sanguo_tw', 'sanguo_ks', 'sanguo_kr', 'dancer_tw', 'dancer_pub']
    dfs = []
    for item in dblist:
        db = str(item)
        df = daily_report(date, db)
        print df
        dfs.append(df)
    all_game = pd.concat(dfs)
    print all_game


"""
应该如何简化这个脚本
# 当日付费用户&当日付费&ARPPU相关
# dau&当日收入&ARPU&当日付费用户&ARPPU相关
df2 = pd.pivot_table(df1, index=['ds'], values=['user_id', 'today_pay'], aggfunc={
                        'user_id': len, 'today_pay': np.sum}).reset_index().rename(columns={'user_id': 'dpu'})
df2['ARPPU'] = (df2.today_pay / df2.dpu).round(2)
df['涨跌'] = map(lambda x: '涨' if x > 0 else (
    '跌' if x < 0 else '平'), df['p_change'])
df['no'] = map(lambda x: 'new' if x ==
                '2017-04-08' else 'old', df.reg_time)
def do_merchant(x,y):
    return y/x  
A_2Vehicle_count['vehicle_count']=map(lambda x,y:do_merchant(x,y),A_2Vehicle_count['ave_time'],A_2Vehicle_count['sum_time']) 
"""
