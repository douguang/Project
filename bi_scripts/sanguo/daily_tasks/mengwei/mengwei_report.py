#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-3-23 下午6:50
@Author  : Andy 
@File    : mengwei_report.py
@Software: PyCharm
Description :  孟玮report的短期按月

月份  日平均新增用户 日平均DAU  日平均充值人数  日平均收入 日平均付费率 日平均ARPU 日平均ARPPU 日平均新增付费玩家数 日平均付费老用户数  月总收入
'''

from utils import hql_to_df, ds_add, date_range
import settings_dev
import pandas as pd

def mengwei_report(platform):
    settings_dev.set_env(platform)
    print platform

    # ##月平均DAU

    # 每日DAU
    ds_dau_sql = '''
            select ds,count(distinct user_id) as dau from raw_info where ds>='20160419' group by ds
        '''
    print ds_dau_sql
    ds_dau_df = hql_to_df(ds_dau_sql).fillna(0)
    # print ds_dau_df.head(3)
    # 每日新增
    ds_dnu_sql = '''
            select regexp_replace(substring(reg_time,1,10),'-','') as ds,count(distinct user_id) as dnu from mid_info_all where ds='20170321' group by ds
        '''
    print ds_dnu_sql
    ds_dnu_df = hql_to_df(ds_dnu_sql).fillna(0)
    # print ds_dnu_df.head(3)
    # 每日付费次数 付费人数
    ds_pay_sql = '''
                select ds,sum(order_money) as order_money,count(*) as pau,count(distinct user_id) as pun from raw_paylog  where ds>='20160419' and platform_2 != 'admin_test' group by ds
        '''
    print ds_pay_sql
    ds_pay_df = hql_to_df(ds_pay_sql).fillna(0)
    # print ds_pay_df.head(3)

    result_df = ds_dau_df.merge(ds_dnu_df, on=['ds',]).fillna(0)
    result_df = result_df.merge(ds_pay_df, on=['ds',]).fillna(0)
    # 付费率 ARPU ARPPU
    result_df['pay_rate'] = result_df['pun']/result_df['dau']
    result_df['ARPU'] = result_df['order_money']/result_df['dau']
    result_df['ARPPU'] = result_df['order_money']/result_df['pun']
    print result_df.head(3)
    result_df['ds'] = pd.DataFrame((x[0:6] for x in result_df.ds), index=result_df.index,)
    print result_df.head(3)
    result_df = result_df.groupby(['ds',]).agg({
        'dau': lambda g: g.mean(),
        'dnu': lambda g: g.mean(),
        'order_money': lambda g: g.sum(),
        'pau': lambda g: g.mean(),
        'pun': lambda g: g.mean(),
        'pay_rate': lambda g: g.mean(),
        'ARPU': lambda g: g.mean(),
        'ARPPU': lambda g: g.mean(),
    }).reset_index()

    result_df.to_excel('/home/kaiqigu/桌面/%s-按月统计2.xlsx' % platform,index=False)
if __name__ == '__main__':
    platform = 'sanguo_ks'
    mengwei_report(platform)