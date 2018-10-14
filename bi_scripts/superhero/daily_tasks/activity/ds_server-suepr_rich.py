#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: ds_server-suepr_rich.py 
@time: 18/3/20 下午6:58 
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
        select t1.ds,t1.server,t1.uid,t1.coin_num,row_number() over(partition by t1.ds,t1.server  order by t1.coin_num desc)as rn  from (
        select ds,reverse(substr(reverse(uid),8)) as server,uid,sum(coin_num) as coin_num
        from raw_spendlog where ds>='20180315' group by ds,server,uid
        )t1 
        group by t1.ds,t1.server,t1.uid,t1.coin_num
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
        result.to_excel(r'/Users/kaiqigu/Documents/Superhero/超英-英文版-分服分天-宇宙最强_20180320.xlsx', index=False,encoding='utf-8')
    print "end"