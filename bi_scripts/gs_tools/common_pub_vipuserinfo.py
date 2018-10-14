#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Author      : Hu Chunlong
Description : 导入超级英雄国内版（pub&qiku）VIP用户信息至VIP系统
create_date : 2016.12.27
"""
from utils import hql_to_df, update_mysql
import settings_dev
import pandas as pd
import datetime


def common_pub_vipuserinfo(date):
    info_df = pd.read_excel(
        r'C:\workflow\bi_scripts\gs_tools\common_pub_vipuserinfo.xlsx')
    sql = '''
    SELECT uid as role_id,
           nick as role_name,
           level,
           vip_level,
           to_date(fresh_time) AS act_time
    FROM mid_info_all
    WHERE ds = '{date}'
      AND vip_level >= 5
      AND level > 10
      AND to_date(fresh_time) >= '2016-01-01'
      AND uid NOT IN
       ( SELECT uid
        FROM raw_paylog
        WHERE platform_2 = 'admin_test' )
    '''.format(date=date)
    dfs = []
    for platform in ['superhero_bi', 'superhero_qiku']:
        settings_dev.set_env(platform)
        df = hql_to_df(sql)
        dfs.append(df)
    concat_df = pd.concat(dfs)
    result_df = concat_df.merge(info_df, on='role_id', how='left')
    columns = ['vip_username', 'vip_birthday', 'vip_qq', 'vip_telephone',
               'vip_wechat', 'vip_last_time_conn', 'role_id', 'role_name',
               'level', 'vip_level', 'act_time']
    result_df = result_df[columns]
    table = 'common_pub_vip_info_for_gs'
    del_sql = "DELETE FROM '{0}'".format(table)
    update_mysql(table, result_df, del_sql, 'godvs')


if __name__ == '__main__':
    # 自动获取日期并格式化
    date = (
        datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    common_pub_vipuserinfo(date)
