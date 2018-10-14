#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site: 分服分天钻石摩天轮
@software: PyCharm 
@file: ds_server-diamond_wheel.py 
@time: 18/3/20 下午4:39 
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
        select ds,reverse(substr(reverse(uid),8)) as server,uid,action,act_time,rc,pre_vip as vip,args
        from raw_action_log where ds>='20180315' and action = 'server_diamond_wheel.open_diamond_wheel' and rc not like '%error%' group by ds,server,uid,action,act_time,rc,vip,args
    '''
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head()
    def plat_lines():
        for _, row in info_df.iterrows():
            args_type = row.args
            # print eval(args_type)['type'][0]
            args_type = eval(args_type)['type'][0]
            yield [row.ds,row.uid,row.server,row.act_time,row.action,row.rc,row.vip,row.args,args_type]

    res_df = pd.DataFrame(plat_lines(), columns=['ds', 'uid', 'server','act_time', 'action', 'rc','vip', 'args', 'args_type',])
    print res_df.head()

    result1_df = res_df.groupby(['ds', 'vip', 'args_type',]).agg({
        'uid': lambda g: g.nunique(),
    }).reset_index().rename(columns={'uid': 'uid_num',})
    result2_df = res_df.groupby(['ds', 'vip', 'args_type', ]).agg({
        'uid': lambda g: g.count(),
    }).reset_index().rename(columns={'uid': 'num',})

    result_df = result1_df.merge(result2_df, on=['ds', 'vip', 'args_type',], how='left')
    return result_df

if __name__ == '__main__':

    for platform in ['superhero_mul',]:
        settings_dev.set_env(platform)
        result = data_reduce()
        result.to_excel(r'/Users/kaiqigu/Documents/Superhero/超英-英文版-分服分VIP-钻石摩天轮_20180320.xlsx', index=False,encoding='utf-8')
    print "end"