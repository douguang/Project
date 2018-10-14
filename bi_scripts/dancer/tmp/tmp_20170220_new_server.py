#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 新服开服前14天的DAU、DNU、充值情况
Database    : dancer_pub
'''
import settings_dev
import pandas as pd
from utils import hql_to_df, date_range, ds_add

def tmp_20170220_new_server(start_date, date):

    result_sql = '''
        select t1.server, t1.dau, t2.dnu, t3.pay, t3.pay_num, t1.ds from (
            select count(distinct account) as dau, ds, reverse(substr(reverse(user_id), 8)) as server from parse_info where ds>='{start_date}' and ds<='{date}' group by ds, server) t1
        left join (
            select count(distinct account) as dnu, regexp_replace(to_date(reg_time),'-','') as regtime, reverse(substr(reverse(user_id), 8)) as server from parse_info where ds>='{start_date}' and ds<='{date}' group by regtime, server) t2
        on (t1.server=t2.server and t1.ds=t2.regtime) left join (
            select sum(order_money) as pay, count(user_id) as pay_num, ds, reverse(substr(reverse(user_id), 8)) as server from raw_paylog where ds>='{start_date}' and ds<='{date}' and platform_2 != 'admin_test' and order_id not like '%testktwwn%' group by ds, server) t3
        on (t1.server=t3.server and t1.ds=t3.ds)
    '''.format(start_date=start_date, date=date)
    print result_sql
    result_df = hql_to_df(result_sql).fillna(0)
    print result_df.head(25)

    result_df['rank'] = result_df['ds'].groupby(result_df['server']).rank()
    result_df = result_df[result_df['rank'] <= 14]
    result_df = result_df.sort(['server', 'ds']).reset_index()
    print result_df.head(25)
    return result_df

if __name__ == '__main__':
    for platform in ('dancer_pub',):
        settings_dev.set_env(platform)
        result = tmp_20170220_new_server('20170301', '20170327')
        result.to_excel(r'E:\Data\output\dancer\new_server_%s.xlsx'%platform)
