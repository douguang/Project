#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-4-6 上午10:11
@Author  : Andy 
@File    : shenji2.py
@Software: PyCharm
Description :
'''
from utils import date_range, ds_add, hql_to_df
import settings_dev
import pandas as pd


def tw_ltv():
    rank_sql = '''
    select t2.user_id,t2.order_money,t2.rank
    from(
        select t1.user_id,t1.order_money,row_number() over(order by t1.order_money  DESC ) as rank
        from(
        select user_id,sum(order_money)  as order_money
        from raw_paylog
        where ds>='20160419'
        and ds<='20161231'
        and platform_2 != 'admin_test'
        and platform_2 != 'admin'
        group by user_id
        order by order_money desc
        )t1
        group by t1.user_id,t1.order_money
    )t2
    where t2.rank<=100
    group by t2.user_id,t2.order_money,t2.rank
    '''
    print rank_sql
    rank_df = hql_to_df(rank_sql)
    print rank_df.head()

    name_sql ='''
         select user_id,name from mid_info_all where ds='20161231'  group by user_id,name
    '''
    print name_sql
    name_df = hql_to_df(name_sql)
    print name_df.head()
    name_df.to_excel(r'/home/kaiqigu/桌面/机甲无双_金山-前100-name.xlsx', index=False)

    ip_sql='''
         select user_token as user_id,ip from parse_nginx where ds>='20160419' and user_token != '' group by user_id,ip
    '''
    print ip_sql
    ip_df = hql_to_df(ip_sql)
    print ip_df.head()
    ip_df.to_excel(r'/home/kaiqigu/桌面/机甲无双_金山-前100-ip.xlsx', index=False)

    result_df = rank_df.merge(name_df, on=['user_id', ])
    result_df = result_df.merge(ip_df, on=['user_id', ],)
    result_df.to_excel(r'/home/kaiqigu/桌面/机甲无双_金山-前100.xlsx', index=False)
if __name__ == '__main__':
    for platform in ['sanguo_ks']:
        settings_dev.set_env(platform)
        tw_ltv()
    print 'end'
