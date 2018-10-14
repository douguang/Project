#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-6-22 下午9:22
@Author  : Andy 
@File    : language_server_new_old_pay.py
@Software: PyCharm
Description :   新老服收入
'''

from utils import date_range, ds_add, hql_to_df
import settings_dev
import pandas as pd
from ipip import *
def data_reduce(start_date,end_date):
    print start_date,end_date
    raw_info_sql='''
        select user_id,language,reverse(substring(reverse(user_id),8)) as server_id from mid_info_all where ds='{end_date}' group by user_id,language,server_id
    '''.format(end_date=end_date)
    print raw_info_sql
    raw_info_df = hql_to_df(raw_info_sql)
    print raw_info_df
    rep_dic = {None: '泰语', '0': '英文', '1': '简中', '2': '繁中', '3': '泰语', '4': '越南语', '5': '印尼语'}
    raw_info_df['language'] = raw_info_df.replace(rep_dic).language
    print raw_info_df

    pay_sql = '''
        select ds,user_id,sum(order_money) as order_money from raw_paylog where ds>='{start_date}' and ds<='{end_date}' and platform_2 != 'admin_test' group by ds,user_id
    '''.format(start_date=start_date,end_date=end_date)
    print pay_sql
    pay_df = hql_to_df(pay_sql)
    print pay_df

    server_act_sql = '''
        select reverse(substring(reverse(user_id),8)) as server_id,min(ds) as frist_ds from raw_info where ds>='20170103' group by server_id
    '''
    print server_act_sql
    server_act_df = hql_to_df(server_act_sql)
    print server_act_df

    result_df = pay_df.merge(raw_info_df, on=['user_id', ], how='left')
    result_df = result_df.merge(server_act_df, on=['server_id', ], how='left')
    print result_df

    result_df.to_excel(r'/home/kaiqigu/桌面/机甲无双-多语言-收入-先分语言再分新老服_20170622.xlsx', index=False)
if __name__ == '__main__':
    settings_dev.set_env('sanguo_tl')
    data_reduce('20170103','20170621')


