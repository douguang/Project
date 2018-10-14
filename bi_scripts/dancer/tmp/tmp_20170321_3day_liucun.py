#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 自3月1日起至现在流失玩家的人数，VIP等级，日储值金额，流失等级（流失判断：1登陆、2未登陆、3未登陆即算流失）
'''
import pandas as pd
from utils import hql_to_df, ds_add, date_range
import settings_dev

def tmp_20170321_3day_liucun(date):

    daily_sql = '''
        select t1.user_id, t1.vip, t1.level, t2.pay, t1.ds from (
        select user_id, vip, level, ds from parse_info where ds='{date}' and user_id not in
        (select user_id from parse_info where ds = '{date_1}') and user_id not in (select user_id from parse_info where ds = '{date_2}')) t1
        left join (select user_id, sum(order_money) as pay from raw_paylog where ds='{date}' and platform_2 != 'admin_test' and order_id not like '%testktwwn%' group by user_id) t2
        on t1.user_id = t2.user_id
    '''.format(date=date, date_1=ds_add(date, 1), date_2=ds_add(date, 2))
    print daily_sql
    daily_df = hql_to_df(daily_sql)
    print daily_df.head(10)
    return daily_df

if __name__ == '__main__':
    for platform in ['dancer_tw']:
        settings_dev.set_env(platform)
        result_list = []
        for date in date_range('20170301', '20170320'):
            result_list.append(tmp_20170321_3day_liucun(date))
        result = pd.concat(result_list)
        result.to_excel(r'E:\Data\output\dancer\daily_date.xlsx')