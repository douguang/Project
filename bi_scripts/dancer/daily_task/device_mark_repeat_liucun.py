#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: device_mark_repeat_liucun.py
@time: 18/1/30 下午6:29 
"""

#!/usr/bin/env python
# encoding: utf-8

""" 
@author: Andy 
@site:  设备码重合
@software: PyCharm 
@file: device_mark_repeat.py 
@time: 18/1/30 上午11:03 
"""


import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range
import datetime
import pandas as pd
import sys
reload(sys)
sys.setdefaultencoding('utf8')


def data_reduce():
    settings_dev.set_env('dancer_cgame')

    info_cgame_sql = '''
    select ds,device_mark,account,user_id,to_date(reg_time)as reg_time  from parse_info where ds>='20180124' and to_date(reg_time)  >='2018-01-24'  group by ds,device_mark,account,user_id,reg_time 
    '''
    print info_cgame_sql
    info_cgame_df = hql_to_df(info_cgame_sql)
    print info_cgame_df.head()

    settings_dev.set_env('dancer_pub')
    info_pub_sql = '''
        select device_mark,account as account_pub from mid_info_all where ds='20180129' and  act_time >='2017-10-29 00:00:00' and   device_mark != 'ff:ff:ff:ff:ff:ff' and   device_mark != '02:00:00:00:00:00' and  device_mark != '00:00:00:00:00:00' and device_mark != '00000000-0000-0000-0000-000000000000' group by device_mark,account_pub
        '''
    #    where t1.device_mark != '00000000-0000-0000-0000-000000000000'
    print info_pub_sql
    info_pub_df = hql_to_df(info_pub_sql)
    print info_pub_df.head()

    settings_dev.set_env('dancer_bt')
    info_bt_sql = '''
            select device_mark,account as account_bt from mid_info_all where ds='20180129' and  act_time >='2017-12-05 00:00:00' and   device_mark != 'ff:ff:ff:ff:ff:ff' and  device_mark != '02:00:00:00:00:00' and  device_mark != '00:00:00:00:00:00' and device_mark != '00000000-0000-0000-0000-000000000000' group by device_mark,account_bt
            '''
    #    where t1.device_mark != '00000000-0000-0000-0000-000000000000'
    print info_bt_sql
    info_bt_df = hql_to_df(info_bt_sql)
    print info_bt_df.head()

    pay_info_sql = '''
        select ds,user_id,sum(order_money) as order_money from raw_paylog where ds>='20180124' and platform_2 != 'admin_test' group by ds,user_id
    '''
    print pay_info_sql
    pay_info_df = hql_to_df(pay_info_sql)
    print pay_info_df.head()

    info_cgame_pub_df = info_cgame_df[info_cgame_df['device_mark'].isin(info_pub_df.device_mark)]
    info_cgame_bt_df = info_cgame_df[info_cgame_df['device_mark'].isin(info_bt_df.device_mark)]

    info_cgame_pub = info_cgame_pub_df.groupby(['reg_time','ds',]).agg({
        'account': lambda g: g.nunique(),
    }).reset_index().rename(columns={'account': 'cgame_account_pub_num', })
    info_cgame_pub.to_excel(r'/Users/kaiqigu/Documents/Dancer/2武娘-cgame-deivcemark重合数据-留存-info_cgame_pub_20180130.xlsx', index=False)

    info_cgame_bt = info_cgame_bt_df.groupby(['reg_time','ds',]).agg({
        'account': lambda g: g.nunique(),
    }).reset_index().rename(columns={'account': 'cgame_account_bt_num',})
    info_cgame_bt.to_excel(r'/Users/kaiqigu/Documents/Dancer/2武娘-cgame-deivcemark重合数据-留存-info_cgame_bt_20180130.xlsx', index=False)

    info_cgame_pub_pay_df = info_cgame_pub_df.merge(pay_info_df, on=['ds','user_id',], how='left')
    info_cgame_bt_pay_df = info_cgame_bt_df.merge(pay_info_df, on=['ds','user_id',], how='left')


    # 收入
    info_cgame_pub_order = info_cgame_pub_pay_df.groupby(['reg_time', 'ds', ]).agg({
        'order_money': lambda g: g.nunique(),
    }).reset_index().rename(columns={'account': 'cgame_account_pub_order_money', })
    info_cgame_pub_order.to_excel(r'/Users/kaiqigu/Documents/Dancer/2武娘-cgame-deivcemark重合数据-留存-info_cgame_pub-order_money_20180130.xlsx',index=False)

    info_cgame_bt_order = info_cgame_bt_pay_df.groupby(['reg_time', 'ds', ]).agg({
        'order_money': lambda g: g.nunique(),
    }).reset_index().rename(columns={'account': 'cgame_account_bt_order_money', })
    info_cgame_bt_order.to_excel(r'/Users/kaiqigu/Documents/Dancer/2武娘-cgame-deivcemark重合数据-留存-info_cgame_bt-order_money_20180130.xlsx',index=False)



if __name__ == '__main__':
    result = data_reduce()
    print "end"