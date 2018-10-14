#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  充值档次分布
@software: PyCharm 
@file: pay_level.py 
@time: 18/3/9 下午2:16 
"""


import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range
import pandas as pd

def data_reduce():
    vt_ranges = [99, 100, 199, 200, 300, 500, 1500, 3000, 5000, 10000, 20000,
                 30000, 40000, 50000, 50000000]

    info_sql = '''
      select ds,uid,sum(order_coin) as order_coin from raw_paylog where ds>='20180201' and platform_2 != 'admin_test' group by ds,uid
    '''
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head()

    info_df['ranges'] = pd.cut(info_df.order_coin,
                                 vt_ranges).astype('object')

    # info_df = (info_df.groupby(['ds','ranges']).agg({'uid': 'count','order_coin': 'sum',}).reset_index().fillna(0).rename(columns={'uid': 'pay_num'}))
    info_df = (info_df.groupby(['ds','ranges']).agg( {'uid': lambda g: g.nunique(),'order_coin': lambda g: g.sum(), }).reset_index().fillna(0).rename(columns={'uid': 'pay_num','order_coin': 'num'}))

    info_df.to_csv(r'/Users/kaiqigu/Documents/Superhero/超英-越南-充值档次分布_20180309-2.csv')

if __name__ == '__main__':

    for platform in ['superhero_vt',]:
        settings_dev.set_env(platform)
        data_reduce()
        # result.to_csv(r'/Users/kaiqigu/Documents/Superhero/超英-越南-箱子数据_20180307.csv')
        # result.to_excel(r'/Users/kaiqigu/Documents/Superhero/超英-越南-箱子数据_20180307-5.xlsx', index=False)
    print "end"