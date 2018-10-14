#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-4-20 下午2:46
@Author  : Andy 
@File    : reg_vip_distribution.py
@Software: PyCharm
Description :   新增VIP分布
'''


import pandas as pd
import settings_dev
from utils import hql_to_df,ds_add, date_range

def data(date):

    reg_sql = '''
        select ds,count(distinct user_id) as dnu from raw_info where ds='{date}' and regexp_replace(substring(reg_time,1,10),'-','') = '{date}' and platform not in ('appstore', 'appstore@ol', 'appstore@cnjtdz') group by ds
    '''.format(date=date,)
    print reg_sql
    reg_df = hql_to_df(reg_sql)
    print reg_df.head()

    vip_sql = '''
        select '{date}' as ds,user_id,max(vip) as vip from raw_info where ds>='{date}' and ds<='{end_date}' and platform not in ('appstore', 'appstore@ol', 'appstore@cnjtdz')  and user_id in (
          select user_id from raw_info where ds = '{date}' and regexp_replace(substring(reg_time,1,10),'-','') = '{date}' group by user_id
        )group by ds,user_id
    '''.format(date=date, end_date=ds_add(date,6))
    print vip_sql
    vip_df = hql_to_df(vip_sql)
    print vip_df.head()

    result_df = vip_df.merge(reg_df, on=['ds', ],how='left')

    return result_df

if __name__ == '__main__':
    res_list = []
    for platform in ['sanguo_ks',]:
        settings_dev.set_env(platform)
        for date in date_range('20170313', '20170419'):
            res = data(date)
            res_list.append(res)

    red_df = pd.concat(res_list)

    result_last = red_df.groupby(['ds', 'dnu', 'vip',]).agg({
        'user_id': lambda g: g.nunique(),
    }).reset_index()
    result_last = result_last.rename(columns={'user_id': 'num',})
    result_last.to_excel('/home/kaiqigu/桌面/机甲无双-金山-VIP_20170420.xlsx', index=False)
    print 'end'