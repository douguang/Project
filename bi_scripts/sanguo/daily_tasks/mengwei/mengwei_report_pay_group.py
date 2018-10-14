#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-3-22 下午5:40
@Author  : Andy 
@File    : mengwei_report_pay_group.py
@Software: PyCharm
Description :  参考孟伟超级英雄版本规划——短期数据驱动   收入来源

   每个玩家的注册日期、首次付费时间、付费总额、VIP等级
'''

from utils import hql_to_df, ds_add, date_range
import settings_dev
import pandas as pd

def mengwei_report_pay_group(platform):
    settings_dev.set_env(platform)
    print platform
    # 付费详情
    pay_detail_sql = '''
            select ds,user_id,order_money,order_id from raw_paylog where ds>='20160419' and platform_2 != 'admin_test' group by ds,user_id,order_money,order_id
        '''
    print pay_detail_sql
    pay_detail_df = hql_to_df(pay_detail_sql).fillna(0)
    print pay_detail_df.head(3)
    pay_detail_df.to_excel('/home/kaiqigu/桌面/%s-付费详情.xlsx' % platform, index=False)

    # 玩家首次付费时间
    pay_first_sql = '''
                select user_id,min(ds) as ds  from raw_paylog where ds>='20160419' and platform_2 != 'admin_test' group by user_id
            '''
    print pay_first_sql
    pay_first_df = hql_to_df(pay_first_sql).fillna(0)
    print pay_first_df.head(3)
    pay_first_df.to_excel('/home/kaiqigu/桌面/%s-首次付费时间.xlsx' % platform, index=False)

    # 玩家注册时间
    # 每日新增
    ds_dnu_sql = '''
            select user_id,regexp_replace(substring(reg_time,1,10),'-','') as ds  from mid_info_all where ds='20170322' group by user_id,ds
        '''
    print ds_dnu_sql
    ds_dnu_df = hql_to_df(ds_dnu_sql).fillna(0)
    print ds_dnu_df.head(3)
    ds_dnu_df.to_excel('/home/kaiqigu/桌面/%s-玩家注册时间.xlsx' % platform, index=False)


if __name__ == '__main__':
    platform = 'sanguo_ks'
    mengwei_report_pay_group(platform)








