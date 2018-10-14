#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  三国的
@software: PyCharm 
@file: sanguo_ks-and-sanguo_bt.py 
@time: 17/9/27 上午10:02 
"""

import settings_dev
import pandas as pd
from utils import hql_to_df
from utils import ds_add
from utils import get_server_days,date_range

def data_reduce():
    settings_dev.set_env('sanguo_bt')
    sanguo_bt_sql = '''
        select user_token as user_id_bt,identifier,device_mark from parse_nginx where ds>='20170924' and user_token != '' and device_mark !='wifi02:00:00:00:00:00' and device_mark !='wifi00:00:00:00:00:00' group by user_id_bt,identifier,device_mark
    '''
    print sanguo_bt_sql
    sanguo_bt_df = hql_to_df(sanguo_bt_sql)
    print sanguo_bt_df.head()

    settings_dev.set_env('sanguo_ks')
    sanguo_ks_sql = '''
        select user_token as user_id_ks,identifier,device_mark from parse_nginx where ds>='20161221' and user_token != '' and device_mark !='wifi02:00:00:00:00:00' and device_mark !='wifi00:00:00:00:00:00' group by user_id_ks,identifier,device_mark
    '''
    print sanguo_ks_sql
    sanguo_ks_df = hql_to_df(sanguo_ks_sql)
    print sanguo_ks_df.head()

    # result = sanguo_bt_df.merge(sanguo_ks_df, on='device_mark', how='left')
    result = sanguo_bt_df.merge(sanguo_ks_df, on='device_mark', how='left')
    return result

if __name__ == '__main__':
    a = data_reduce()
    a.to_excel('/Users/kaiqigu/Documents/Sanguo/机甲无双-变态版的玩家玩国内本的情况_20170927-2.xlsx', index=False)
    print "end"


