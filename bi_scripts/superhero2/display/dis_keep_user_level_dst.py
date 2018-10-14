#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 留存用户等级分布、活跃用户等级分布
'''
from utils import hql_to_df, hqls_to_dfs, ds_add, update_mysql, date_range
import settings_dev

def dis_keep_user_level_dst(date):
    sql = '''
    SELECT ds,
           level,
           count(DISTINCT user_id) AS user_num
    FROM parse_info
    WHERE ds = '{date}'
    GROUP BY ds,level
    ORDER BY level
    '''.format(date=date)
    df = hql_to_df(sql)
    # 更新MySQL表
    table = 'dis_keep_user_level_dst'
    print date, table
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, df, del_sql)

if __name__ == '__main__':
    settings_dev.set_env('superhero2')
    # dis_loss_reg_user_level_dst('20160803')
    for date in date_range('20170512', '20170525'):
        print date
        dis_keep_user_level_dst(date)
