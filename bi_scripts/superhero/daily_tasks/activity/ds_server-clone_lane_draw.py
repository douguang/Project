#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site: 分服分天-众神殿数据
@software: PyCharm 
@file: ds_server-clone_lane_draw.py 
@time: 18/3/20 下午4:40 
"""

import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range
import pandas as pd
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

def data_reduce():
    info_sql = '''
        select ds,pre_vip as vip,count(distinct uid) as user_id_num,count(*) as num from raw_action_log where ds>='20180315' and action='server_clone_lane.draw' and rc not like '%error%' group by ds,vip
    '''
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head()
    return info_df

if __name__ == '__main__':

    for platform in ['superhero_mul',]:
        settings_dev.set_env(platform)
        result = data_reduce()
        # result.to_csv(r'/Users/kaiqigu/Documents/Superhero/超英-越南-箱子数据_20180307.csv')
        result.to_excel(r'/Users/kaiqigu/Documents/Superhero/超英-英文版-分服分VIP-众神殿_20180320.xlsx', index=False,encoding='utf-8')
    print "end"