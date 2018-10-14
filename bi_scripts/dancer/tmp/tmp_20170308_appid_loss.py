#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 武娘国服各appid留存情况
Name        : liucun_dancer
Original    : liucun_dancer
'''
import settings_dev
import pandas as pd
from utils import hql_to_df, update_mysql, get_config, date_range

def tmp_20170308_appid_loss(start_date,date):

    liucun_sql = '''
        select t1.regtime, t1.ds, t1.account, t2.appid from (
        select min(regexp_replace(to_date(reg_time),'-','')) as regtime, account, ds
        from parse_info
        where ds >= '{start_date}' and ds<='{date}' and
            regexp_replace(to_date(reg_time),'-','') >= '{start_date}'
            and account in ( select account from parse_nginx where ds >= '{start_date}' and ds <= '{date}' and method='new_user'
        and (pt='ioskvgames' or appid in('cnwnkvzb', 'ktencent', 'yingyongbao', 'cnwngdt')) group by account)
        group by account, ds) t1
        left join (
        select account, appid from parse_nginx where ds >= '{start_date}' and ds <= '{date}' and method='new_user'
        and (pt='ioskvgames' or appid in('cnwnkvzb', 'ktencent', 'yingyongbao', 'cnwngdt')) group by account, appid
        ) t2 on t1.account=t2.account
    '''.format(date=date, start_date=start_date)
    print liucun_sql
    liucun_df = hql_to_df(liucun_sql)
    print liucun_df.head(25)
    return liucun_df

if __name__ == '__main__':
    settings_dev.set_env('dancer_pub')
    result = tmp_20170308_appid_loss('20170214', '20170323')
    result.to_excel(r'E:\Data\output\dancer\appid_liucun.xlsx')