#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 地域LTV
'''
import pandas as pd
from utils import hql_to_df, ds_add
import settings_dev
from ipip import *

# settings_dev.set_env('dancer_tw')
# df = pd.read_excel(r'G:\My_Data_Library\all_ip\ip_config\ip_country_ltv.xlsx')
# table = 'dis_ip_country_ltv'
# date = '20161125'
# del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
# update_mysql(table, df, del_sql)
ltv_days = [3, 7, 14, 30, 60]


def formatDate(Date, formatType='YYYY-MM-DD'):
    formatType = formatType.replace('YYYY', Date[0:4])
    formatType = formatType.replace('MM', Date[4:6])
    formatType = formatType.replace('DD', Date[-2:])
    return formatType


def dis_ip_country_ltv(date):
    # 根据要求的LTV天数倒推需要的开始日期
    start_date = ds_add(date, -max(ltv_days) + 1)
    dates_to_update = [ds_add(date, -ltv_day + 1) for ltv_day in ltv_days]
    server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    f_start_date = formatDate(start_date)
    f_date = formatDate(date)
    # 注册数据
    reg_ip_sql = '''
    select ds, account, regist_ip
    from mid_info_all
    where ds = '{date}'
      and to_date(reg_time) = '{f_date}'
    '''.format(date=date, f_start_date=f_start_date, f_date=f_date)
    reg_ip_df = hql_to_df(reg_ip_sql)
    reg_ip_df['regist_ip'] = reg_ip_df['regist_ip'].astype(basestring)
    IP.load(os.path.abspath("./tinyipdata_utf8.dat"))

    def ip_lines():
        for _, row in reg_ip_df.iterrows():
            ip = row.regist_ip
            country = IP.find(ip).strip().encode("utf8")
            yield [row.ds, row.account, country]

    reg_columns = ['ds', 'account', 'country']
    reg_df = pd.DataFrame(ip_lines())
    print reg_df

if __name__ == '__main__':
    for platform in ['dancer_tw']:
        settings_dev.set_env(platform)
        # dis_reg_user_ltv('20160917')
        date = '20161125'
        # for date in date_range('20160928', '20161010'):
        dis_ip_country_ltv(date)
