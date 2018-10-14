#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: liu zhifeng 
@site:  礼包领取情况（礼包用数字表示的）
@software: PyCharm
@file: task_dist.py
@time: 2018/5/9 20:33
"""

import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range
import pandas as pd


def data_reduce():
    info_sql = '''
    select log_t, a_typ, a_tar
from parse_action_log
where ds >='20180619'
and a_typ='active.omni_exchange'
order by user_id

    '''
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head()
    # user_id_list,log_t_list,a_tar_list, ds_list, level_list,rn_list = [],[],[],[],[],[]
    log_t_list, a_typ_list, a_tar_list = [], [], []

    for _, row in info_df.iterrows():
        a_tar = eval(row.a_tar)
        print a_tar
        print '-------------------------------'

        task_id = a_tar.get('exchange_id', '')[0]
        log_t = row.log_t
        a_typ = row.a_typ

        log_t_list.append(log_t)
        a_tar_list.append(task_id)
        a_typ_list.append(a_typ)

    result_df = pd.DataFrame(
        {'log_t': log_t_list, 'a_tar': a_tar_list, })

    result_df = result_df.groupby(['a_tar']).agg({
        'log_t': lambda g: g.nunique(),
    }).reset_index().rename(columns={'log_t': 'uid_num'})

    return result_df


if __name__ == '__main__':

    for platform in ['superhero2', ]:
        settings_dev.set_env(platform)
        result = data_reduce()
        # result.to_csv(r'/Users/kaiqigu/Documents/Superhero/超英-越南-箱子格式错误数据_20180307.csv')
        result.to_excel(r'C:\Users\Administrator\Desktop\superhero2__20180623-333.xlsx', index=False)
    print "end"