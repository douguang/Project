#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: liu zhifeng 
@site:  超二-新手引导流失点-分渠道，加platform，不分渠道不加platform
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
select * from (
   select ds, user_id, a_tar, log_t, row_number() over(partition by user_id order by user_id,log_t desc ) as rn 
      from parse_action_log
      where ds = "20180802"
      and  a_typ = 'user.guide' 
      and  a_tar like '%guide_id%'
      and  platform = '11'
      and user_id not in (
            select user_id from parse_info where ds='20180803' and to_date(reg_time)='2018-08-02')

group by ds, user_id,a_tar, log_t
)t1
where t1.rn=1
    '''
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head()
    # user_id_list,log_t_list,a_tar_list, ds_list, level_list,rn_list = [],[],[],[],[],[]
    ds_list, user_id_list, a_tar_list, log_t_list = [], [], [], []

    for _, row in info_df.iterrows():
        # print '----------------'
        a_tar = eval(row.a_tar)
        print a_tar
        print '-------------------------------'
        #   task_id = a_tar.get('task_id', '')[0]
        task_id = a_tar.get('guide_id', '')[0]
        uid = row.user_id
        log_t = row.log_t
        ds = row.ds

        # rn = row.rn

        user_id_list.append(row.user_id)
        log_t_list.append(row.log_t)
        a_tar_list.append(task_id)
        ds_list.append(row.ds)

    result_df = pd.DataFrame(
        {'user_id': user_id_list, 'log_t': log_t_list, 'a_tar': a_tar_list, 'ds_list': ds_list, })
    # {'user_id': user_id_list, 'act_time': act_time_list, 'action': action_list, 'demo': demo_list, })

    result_df = result_df.groupby(['a_tar', 'ds_list']).agg({
        'user_id': lambda g: g.nunique(),
    }).reset_index().rename(columns={'user_id': 'uid_num', })

    return result_df


if __name__ == '__main__':

    for platform in ['superhero2', ]:
        settings_dev.set_env(platform)
        result = data_reduce()
        # result.to_csv(r'/Users/kaiqigu/Documents/Superhero/超英-越南-箱子格式错误数据_20180307.csv')
        result.to_excel(r'C:\Users\Administrator\Desktop\superhero2__20180802_liushidian-huawei.xlsx', index=False)
    print "end"