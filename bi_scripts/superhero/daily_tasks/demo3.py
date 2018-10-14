#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: demo3.py 
@time: 18/3/6 下午11:08 
"""

import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range
import pandas as pd

def data_reduce():

    info_sql = '''
      select uid,act_time,action,zhongshendian,args from raw_action_log where ds>='20180305' and action = 'item.use' and args like '%100001%'   and act_time>='2018-03-05 18:15:00' group by uid,act_time,action,zhongshendian,args
    '''
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head()
    # info_df.to_excel(r'/Users/kaiqigu/Documents/Superhero/超英-越南-箱子数据-元数据_20180307.xlsx',index=False)

    user_id_list,act_time_list,action_list,demo_list,num_list = [],[],[],[],[]
    for _, row in info_df.iterrows():
        # print '----------------'
        args = eval(row.args)
        item_id = args.get('item_id','')
        if int(item_id[0]) != 100001:
            continue
        # print row
        uid = row.uid
        act_time = row.act_time
        action = row.action
        data_dic = dict()
        # if 'min_fate_sto15' not in row.zhongshendian:
        # if isinstance(dict(), action)

        try:
            data_dic = eval(row.zhongshendian)
        except:
            print '=Error'
            print row.uid
            print row.act_time
            print row.zhongshendian
            user_id_list.append(row.uid)
            act_time_list.append(row.act_time)
            action_list.append(row.action)
            demo_list.append(row.zhongshendian)


    result_df = pd.DataFrame(
        {'user_id': user_id_list, 'act_time': act_time_list, 'action': action_list, 'demo': demo_list,})

    return result_df

if __name__ == '__main__':

    for platform in ['superhero_vt',]:
        settings_dev.set_env(platform)
        result = data_reduce()
        # result.to_csv(r'/Users/kaiqigu/Documents/Superhero/超英-越南-箱子格式错误数据_20180307.csv')
        result.to_excel(r'/Users/kaiqigu/Documents/Superhero/超英-越南-箱子格式错误数据_20180307.xlsx', index=False)
    print "end"