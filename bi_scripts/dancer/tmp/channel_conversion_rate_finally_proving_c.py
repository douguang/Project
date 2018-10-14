#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-8 下午5:46
@Author  : Andy 
@File    : channel_conversion_rate_finally_proving_c.py
@Software: PyCharm
Description :
'''

from utils import hql_to_df, ds_add, update_mysql, hql_to_df,date_range
import settings_dev
import pandas as pd

def get_data(start_date,end_date):
    demo_sql = '''
        select   t1.account,t1.device,t1.reg_ds,t1.device,t2.user_id,t2.platform
        from(
                select account,device_mark as device,regexp_replace(substr(reg_time,1,11),'-','')as reg_ds
                from parse_info
                where ds>='{start_date}'
                and regexp_replace(substr(reg_time,1,11),'-','') >='{start_date}'
                and ds<='{end_date}'
                and (device_mark = "02:00:00:00:00:00" or device_mark = '00:00:00:00:00:00' or device_mark = '')
                group by device_mark,reg_ds,account
          )t1
          left outer join(
            select account,user_id,platform
            from parse_actionlog
            where ds>="{start_date}"
            and ds<='{end_date}'
            group by account,user_id,platform
            order by account,user_id,platform
            )t2 on t1.account=t2.account
        group by    t1.account,t1.device,t1.reg_ds,t1.device,t2.user_id,t2.platform
    '''.format(start_date = start_date,end_date = end_date)
    print demo_sql
    demo_df = hql_to_df(demo_sql)


    pd.DataFrame(demo_df).to_excel('/home/kaiqigu/桌面/hive中的设备数据(模拟器渠道设备).xlsx',index=False)

# date_list = date_range(start_date,end_date)
if __name__ == '__main__':
    settings_dev.set_env('dancer_pub')
    get_data("20161109","20161207")
    print "end"

