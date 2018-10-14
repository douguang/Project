#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Author      : Hu Chunlong
Description : 导入三国国内版VIP用户信息至VIP系统
create_date : 2016.12.09
"""
from utils import hql_to_df, update_mysql
import settings_dev
import pandas as pd
import datetime


def country_ks_vipuserinfo(date):
    info_df = pd.read_excel(
        r'C:\workflow\bi_scripts\gs_tools\country_ks_vipuserinfo.xlsx')
    sql = '''
    SELECT user_id as role_id,
           name as role_name,
           level,
           vip as vip_level,
           to_date(act_time) AS act_time
    FROM mid_info_all
    WHERE ds = '{date}'
      AND vip >= 6
      AND level > 10
      AND user_id NOT IN
       ( SELECT user_id
        FROM raw_paylog
        WHERE platform_2 = 'admin_test' )
    '''.format(date=date)
    df = hql_to_df(sql)
    result_df = df.merge(info_df, on='role_id', how='left')
    columns = ['vip_username', 'vip_birthday', 'vip_qq', 'vip_telephone',
               'vip_wechat', 'vip_last_time_conn', 'role_id', 'role_name',
               'level', 'vip_level', 'act_time']
    result_df = result_df[columns]
    table = 'country_ks_vip_info_for_gs'
    del_sql = "DELETE FROM '{0}'".format(table)
    update_mysql(table, result_df, del_sql, 'godvs')


if __name__ == '__main__':
    # 自动获取日期并格式化
    date = (
        datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    settings_dev.set_env('sanguo_ks')
    country_ks_vipuserinfo(date)
