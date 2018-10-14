#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-3-23 下午6:33
@Author  : Andy 
@File    : mengwei_report_pay_detail.py
@Software: PyCharm
Description : 参考孟伟超级英雄版本规划——短期数据驱动 玩家充值购买的商品种类
'''

from utils import hql_to_df, ds_add, date_range
import settings_dev
import pandas as pd

def mengwei_report_pay_detail(platform):
    settings_dev.set_env(platform)
    print platform
    # 付费详情
    pay_detail_sql = '''
            select ds,user_id,order_money,product_id,order_id from raw_paylog where ds>='20160419' and platform_2 != 'admin_test' group by ds,user_id,order_money,product_id,order_id
        '''
    print pay_detail_sql
    pay_detail_df = hql_to_df(pay_detail_sql).fillna(0)
    print pay_detail_df.head(3)
    pay_detail_df.to_excel('/home/kaiqigu/桌面/%s-付费商品详情.xlsx' % platform, index=False)


if __name__ == '__main__':
    platform = 'sanguo_tw'
    mengwei_report_pay_detail(platform)
    print 'end'
