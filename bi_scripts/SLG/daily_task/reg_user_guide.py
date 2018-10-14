#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  新手引导停留
@software: PyCharm 
@file: reg_user_guide.py 
@time: 18/1/20 下午3:51 
"""

from utils import hqls_to_dfs, get_rank, hql_to_df, date_range
import pandas as pd
from utils import ds_delta
import settings_dev
from pandas import DataFrame
import time
import datetime


def data_reduce(date):
    guide_sql = '''
            select uid, account, guide_nodes,app_id from parse_info where ds='{0}'  and to_date(reg_time)='2018-01-19' 
        '''.format(date)
    print guide_sql
    guide_df = hql_to_df(guide_sql)
    uid, account, guide,app_id_list = [], [], [],[]
    for _, row in guide_df.iterrows():
        guide_nodes = eval(row['guide_nodes'])
        guide_id = ''
        if type(guide_nodes) == list and len(guide_nodes) != 0:
            guide_id = max(guide_nodes)
        elif type(guide_nodes) == dict:
            value_list = []
            for i in guide_nodes.keys():
                value_list.append(guide_nodes[i])
            guide_id = max(value_list)
        uid.append(row.uid)
        account.append(row.account)
        guide.append(guide_id)
        app_id_list.append(row.app_id)
    result = pd.DataFrame({'uid': uid, 'account': account, 'guide': guide, 'app_id': app_id_list})

    result_df = result.groupby(['app_id','guide']).agg({
        'account': lambda g: g.nunique(),
        'uid': lambda g: g.nunique(),
    }).reset_index().rename(columns={'uid': 'uid_num','account': 'account_num', })

    return result_df



if __name__ == '__main__':
    for platform in ['slg_mul', ]:
        settings_dev.set_env(platform)
        date = '20180119'
        res = data_reduce(date)
        res.to_excel(r'/Users/kaiqigu/Documents/slg/星战帝国-多语言-新手引导停留2_20180120.xlsx', index=False)
    print "end"