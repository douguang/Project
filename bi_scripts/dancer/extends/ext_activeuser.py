#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 活跃用户数据加工表
'''
from utils import hql_to_df, hqls_to_dfs, ds_add, date_range
import settings_dev

def ext_activeuser(date):
    act_sql = '''
    SELECT user_id,
           account,
           reg_time,
           act_time,
           substr(account,1,instr(account,'_')-1) platform
    FROM parse_info
    WHERE ds ='{0}'
     '''.format(date)
    act_df = hql_to_df(act_sql)
    # 导出数据到指定文件
    act_df.to_csv('/home/data/{plat}/redis_stats/ext_activeuser_{date}'.format(plat = settings_dev.code,date=date), sep = '\t')

if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
