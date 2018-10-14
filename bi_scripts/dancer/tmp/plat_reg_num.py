#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-22 下午7:15
@Author  : Andy 
@File    : plat_reg_num.py
@Software: PyCharm
Description :  渠道新增人数 每天
'''

from utils import hql_to_df, ds_add, update_mysql, hql_to_df,date_range
import settings_dev
import pandas as pd

def get_data():
    reg_sql = '''
        select t1.ds,t2.platform,count(distinct t1.user_id) as player_num
        from(
        select regexp_replace(substr(reg_time,1,10),'-','') as ds,user_id
        from mid_info_all
        where ds="20161221"
        group by regexp_replace(substr(reg_time,1,10),'-',''),user_id
        )t1
        left outer join(
          select user_id,platform
          from parse_actionlog
          where ds>="20161110"
          group by user_id,platform
          )t2 on t1.user_id = t2.user_id
        group by t1.ds,t2.platform
    '''
    print reg_sql
    reg_df = hql_to_df(reg_sql)

    pd.DataFrame(reg_df).to_excel('/home/kaiqigu/桌面/武娘_PUB_分渠道新增.xlsx',index=False)

# date_list = date_range(start_date,end_date)
if __name__ == '__main__':
    settings_dev.set_env('dancer_pub')
    get_data()
    print "end"
#result_df.to_excel('/Users/kaiqigu/Downloads/Excel/d3_ltv.xlsx')

