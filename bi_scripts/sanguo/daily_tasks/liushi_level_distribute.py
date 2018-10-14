#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  流失等级分布
@software: PyCharm 
@file: liushi_level_distribute.py 
@time: 17/9/8 下午5:35 
"""

from utils import date_range, ds_add, hql_to_df
import settings_dev
import pandas as pd


def data_reduce():
    info_sql = '''
        select t1.user_id,t1.level,t2.order_money from (
          select user_id,level from mid_info_all where ds='20170730' and act_time <='2017-05-01 00:00:00'   group by user_id,level
        )t1  left outer join(
          select user_id,sum(order_money) as order_money from raw_paylog where ds>='20160419' and platform_2 !='admin_test' and platform_2 !='admin'  group by user_id
        )t2 on t1.user_id=t2.user_id
        where reverse(substring(reverse(t1.user_id),8)) = 'm1'
        group by  t1.user_id,t1.level,t2.order_money
    '''
    print info_sql
    info_df = hql_to_df(info_sql).fillna(0)
    print info_df.head(10)

    # info_df = info_df[~(info_df['order_money'] > 6)]
    info_df = info_df[info_df['order_money'] < 1000]
    print info_df.head(10)
    level_list = range(0, 201, 10)
    result_df = info_df.groupby(
        [pd.cut(info_df.level, level_list),]).agg(
        {'user_id': lambda g: g.nunique(),}).reset_index()
    result_df.level = result_df.level.astype('object')


    result_df.to_excel(r'/home/kaiqigu/桌面/机甲无双-金山-近三个月未登录用户的等级分布_20170731()m1.xlsx', index=False)


if __name__ == '__main__':
    for platform in ['sanguo_ks',]:
        settings_dev.set_env(platform)
        data_reduce()
    print "end"
