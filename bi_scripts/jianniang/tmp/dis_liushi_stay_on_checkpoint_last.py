#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-3-20 下午3:02
@Author  : Andy 
@File    : dis_liushi_stay_on_checkpoint_last.py
@Software: PyCharm
Description :   次日流失的最后停留关卡
'''

from utils import hql_to_df, ds_add, date_range
import settings_dev
import pandas as pd

def dis_liushi_stay_on_checkpoint_last(date):
    print date
    # 流失用户
    ciliushi_sql = '''
        select ds,user_id from raw_info where ds='{date}' and regexp_replace(substring(reg_time,1,10),'-','')  = '{date}'  and user_id not in (select user_id from raw_info where ds='{tomor_date}' group by user_id)  group by ds,user_id
    '''.format(date=date,tomor_date=ds_add(date,1))
    print ciliushi_sql
    ciliushi_df = hql_to_df(ciliushi_sql).fillna(0)

    # 用户当日的最后停留关卡
    last_raids_sql='''
        select ds,user_id,raids from raw_raid where ds='{date}'
    '''.format(date=date)
    print last_raids_sql
    last_raids_df = hql_to_df(last_raids_sql).fillna(0)
    result_list=[]
    ds_list,user_id_list,rais_list=[],[],[]
    for i in range(len(last_raids_df)):
        ds = last_raids_df.iloc[i, 0]
        user_id = last_raids_df.iloc[i, 1]
        tar = last_raids_df.iloc[i, 2]

        tar = tar.replace('[','').replace(']','').strip()
        if tar != '':
            raids_list = tar.split(',')
            if raids_list[-1] == '':
                last_raids=raids_list[-2]
            else:
                last_raids = raids_list[-1]
            ds_list.append(ds)
            user_id_list.append(user_id)
            rais_list.append(last_raids)

    last_raids_df = pd.DataFrame({'ds': ds_list,
                     'user_id': user_id_list,
                     'raids':rais_list,})

    result_df = ciliushi_df.merge(last_raids_df, on=['ds', 'user_id', ]).fillna(0)
    result_df = pd.DataFrame(result_df).groupby(['ds', 'raids']).agg(
        {'user_id': lambda g: g.nunique()}).reset_index()
    result_df = result_df.rename(
        columns={'user_id': 'user_id_num', })
    print result_df.head(3)

    result_df.to_excel('/home/kaiqigu/桌面/剑娘-测试-次日流失的最后停留关卡.xlsx', index=False)
if __name__ == '__main__':
    platform = 'jianniang_test'
    date = '20170318'
    settings_dev.set_env(platform)
    dis_liushi_stay_on_checkpoint_last(date)