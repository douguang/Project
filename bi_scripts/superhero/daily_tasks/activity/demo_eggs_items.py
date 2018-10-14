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
import pandas as pd
import xlwt
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range, get_config
import json
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

def data_reduce():
    info_sql = '''
              
        select t1.uid, t1.pre_vip, t1.args, t2.money from 
        (
          
        select uid, pre_vip, args from raw_action_log where action = 'egg.open_egg' and act_time<'2018-04-22 00:00:00' and act_time>'2018-04-21 00:00:00'and ds = '20180421' and rc = 0.0
        
          )t1 left join 
          (
            select uid, sum(order_money) as money from raw_paylog where ds = '20180421' group by uid
            )t2 on t1.uid = t2.uid
    
    '''


    print info_sql
    info_df = hql_to_df(info_sql)

    def super_types_lines():
        for _, row in info_df.iterrows():
            # print row
            arg = eval(row.args)
            is_super_tmp = arg.get('is_super')
            is_super = is_super_tmp[0]
            egg_type_tmp = arg.get('egg_type')
            egg_type = egg_type_tmp[0]
            yield [row.uid, row.pre_vip, is_super, egg_type, row.money]

    user_info_df = pd.DataFrame(super_types_lines(), columns=[
        'user_id', 'vip', 'is_super', 'egg_type', 'money'])
    user_info_df.to_excel('/Users/kaiqigu/Desktop/BI_excel/20180424/demo.xlsx')

    # print info_df.head()
    # return info_df

if __name__ == '__main__':

    for platform in ['superhero_vt']:
        settings_dev.set_env(platform)
        data_reduce()
        # result.to_csv(r'/Users/kaiqigu/Documents/Superhero/超英-越南-箱子数据_20180307.csv')
        # result.to_excel(r'/Users/kaiqigu/Documents/Superhero/超英-英文版-分服分天-宇宙最强_20180320.xlsx', index=False,encoding='utf-8')
    print "end"