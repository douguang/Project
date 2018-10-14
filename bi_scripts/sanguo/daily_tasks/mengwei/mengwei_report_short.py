#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-3-22 下午3:43
@Author  : Andy 
@File    : mengwei_report_short.py
@Software: PyCharm
Description :  参考孟伟超级英雄版本规划——短期数据驱动
'''

from utils import hql_to_df, ds_add, date_range
import settings_dev
import pandas as pd

def mengwei_report_short(platform):
    settings_dev.set_env(platform)
    print platform
    # 每日活跃
    ds_dau_sql = '''
        select ds,count(distinct user_id) as dau from raw_info where ds>='20160419' group by ds
    '''
    print ds_dau_sql
    ds_dau_df = hql_to_df(ds_dau_sql).fillna(0)
    print ds_dau_df.head(3)
    ds_dau_df.to_excel('/home/kaiqigu/桌面/%s-每日活跃.xlsx' % platform, index=False)

    # 每日新增
    ds_dnu_sql = '''
        select regexp_replace(substring(reg_time,1,10),'-','') as ds,count(distinct user_id) as dnu from mid_info_all where ds='20170321' group by ds
    '''
    print ds_dnu_sql
    ds_dnu_df = hql_to_df(ds_dnu_sql).fillna(0)
    print ds_dnu_df.head(3)
    ds_dnu_df.to_excel('/home/kaiqigu/桌面/%s-每日新增.xlsx'% platform, index=False)

    # 每日付费次数 付费人数
    ds_pay_sql = '''
            select ds,sum(order_money) as order_money,count(*) as pau,count(distinct user_id) as pun from raw_paylog  where ds>='20160419' and platform_2 != 'admin_test' group by ds
    '''
    print ds_pay_sql
    ds_pay_df = hql_to_df(ds_pay_sql).fillna(0)
    print ds_pay_df.head(3)
    ds_pay_df.to_excel('/home/kaiqigu/桌面/%s-每日付费.xlsx'% platform, index=False)


if __name__ == '__main__':
    platform = 'sanguo_tw'
    mengwei_report_short(platform)

