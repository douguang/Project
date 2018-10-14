#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Author      : Hu Chunlong
Description : mart_assist
create_date : 2017.01.16
"""
from utils import hqls_to_dfs, update_mysql, ds_add
import pandas as pd
from pandas import DataFrame
import settings_dev
import datetime
import time
import numpy as np
from mart_sql_cfg import sanguo_parse_actionlog_sql, sanguo_raw_info_sql, sanguo_nginx_sql, paylog_sql, spendlog_sql, sanguo_parse_actionlog_2_sql


def mart_assist(date):
    print 'mart_assist>>>start', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

    df_1, df_2, df_3 = hqls_to_dfs([sanguo_raw_info_sql.format(date=date), sanguo_parse_actionlog_sql.format(
        date=date), paylog_sql.format(date=date)])
    df_2 = df_2.drop_duplicates(['user_id'])
    df_4, df_5 = hqls_to_dfs([spendlog_sql.format(
        date=date), sanguo_parse_actionlog_2_sql.format(date=date)])
    # df_6 = hql_to_df(sanguo_nginx_sql.format(date=date))
    # df_6 = df_3.drop_duplicates(['user_id'])

    ori_df = df_1.merge(df_2, on='user_id', how='left').merge(df_3, on='user_id', how='left').merge(
        df_4, on='user_id', how='left').merge(df_5, on='user_id', how='left')
    # 增加当前日期，分别为YYYYMMDD和YYYY-MM-DD格式
    ori_df['ds'] = date
    ori_df['today'] = pd.Timestamp(date)
    ori_df['reg_time'] = pd.to_datetime(ori_df['reg_time'])
    ori_df['act_time'] = pd.to_datetime(ori_df['act_time'])
    ori_df['first_pay_date'] = pd.to_datetime(ori_df['first_pay_date'])
    ori_df['last_pay_date'] = pd.to_datetime(ori_df['last_pay_date'])
    ori_df['ip'] = ''
    ori_df['device'] = ''
    ori_df = ori_df.fillna({'all_pay': 0, 'today_pay': 0, 'free_coin': 0,
                            'charge_coin': 0, 'spend_coin': 0}, inplace=True)
    columns = ['user_id', 'name', 'server', 'ip', 'platform', 'account', 'device', 'device_mark', 'reg_time', 'act_time', 'level', 'vip', 'combat',
               'first_pay_date', 'last_pay_date', 'all_pay', 'today_pay', 'free_coin', 'charge_coin', 'spend_coin', 'last_act']
    for itype in ['all_pay', 'today_pay', 'free_coin', 'charge_coin', 'spend_coin']:
        ori_df['%s' % itype] = ori_df['%s' % itype].astype('float')
    for ex_itype in ['vip', 'level', 'combat']:
        ori_df['%s' % ex_itype] = ori_df['%s' % ex_itype].astype('int')
    result_df = ori_df[columns]
    print 'mart_assist>>>end', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    return result_df

if __name__ == '__main__':
    settings_dev.set_env('qiling_ks')
    date = '20170116'
    mart_assist(date)
