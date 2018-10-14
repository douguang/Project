#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  分服分天-轮盘
@software: PyCharm 
@file: ds_server-roulette.py 
@time: 18/3/20 下午4:38 
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
        select t1.ds,t1.server,count(distinct t1.uid) as user_num,sum(t1.num) as  num from (
        select ds,reverse(substr(reverse(uid),8)) as server,uid,action,act_time, 
        sum(CASE WHEN action='server_roulette.open_roulette' THEN 1 
                 WHEN action='server_roulette.open_roulette10' THEN 10
            ELSE 0 END)  AS num
        from raw_action_log where ds>='20180315' and action like 'server_roulette.open_roulette%'  and rc not like '%error%' group by ds,server,uid,action,act_time
        )t1 
        group by t1.ds,t1.server  
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
        result.to_excel(r'/Users/kaiqigu/Documents/Superhero/超英-英文版-分服分天-轮盘_20180320.xlsx', index=False,encoding='utf-8')
    print "end"

