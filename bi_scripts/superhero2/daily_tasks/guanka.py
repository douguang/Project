#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: liu zhifeng 
@site:  玩家打到了哪一关（篇章） - 关卡 分 简单 和 困难 两种
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
      select * from(select user_id, log_t, a_tar, ds, level, row_number() over(partition by user_id order by log_t desc) as rn 
      from parse_action_log where ds='20180620' and a_typ = 'private_city.battle_end'  and  a_tar like "%u\\'degree\\': 1%"
      and user_id  in (select  user_id
from raw_paylog
where ds >='20180619'
and  platform <> 'admin_test')	
       ) t1 where t1.rn=1
    '''
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head()

    user_id_list, log_t_list, a_tar_list, ds_list, level_list, rn_list = [], [], [], [], [], []

    for _, row in info_df.iterrows():
        a_tar = eval(row.a_tar)
        task_id = a_tar.get('chapter_id', '')

        uid = row.user_id
        log_t = row.log_t
        ds = row.ds
        level = row.level
        rn = row.rn

        user_id_list.append(row.user_id)
        log_t_list.append(row.log_t)
        a_tar_list.append(task_id)

        ds_list.append(row.ds)
        level_list.append(row.level)
        rn_list.append(row.rn)

    result_df = pd.DataFrame(
        {'user_id': user_id_list, 'log_t': log_t_list, 'a_tar': a_tar_list , 'ds_list': ds_list, 'level': level_list,
         'rn': rn_list, })

    return result_df


if __name__ == '__main__':

    for platform in ['superhero2', ]:
        settings_dev.set_env(platform)
        result = data_reduce()
        result.to_excel(r'E:\super2_20180621-vip-jiandan-0620.xlsx', index=False)
    print "end"