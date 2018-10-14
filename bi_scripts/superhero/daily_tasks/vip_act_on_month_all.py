#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: vip_act_on_month_all.py 
@time: 18/4/2 下午10:15 
"""

import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range
import pandas as pd
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

def data_reduce():
    res_list = []
    for date in ['20171130','20171231','20180131','20180228','20180331']:
        act_mon = ''
        first_ds = ''
        if date == '20171130':
            act_mon = '201711'
            first_ds = '20171101'
        if date == '20171231':
            act_mon = '201712'
            first_ds = '20171201'
        if date == '20180131':
            act_mon = '201801'
            first_ds = '20180101'
        if date == '20180228':
            act_mon = '201802'
            first_ds = '20180201'
        if date == '20180331':
            act_mon = '201803'
            first_ds = '20180301'
        info_sql = '''
          select ds,vip_level, uid from mid_info_all where ds = '{date}' and substr(regexp_replace(to_date(fresh_time),'-',''),1,6)  = '{act_mon}'
          group by ds,vip_level, uid
        '''.format(date=date,act_mon=act_mon,)
        print info_sql
        info_df = hql_to_df(info_sql)

        print 'first_ds:',first_ds
        pay_sql = '''
             select uid,sum(order_money)  as order_money from raw_paylog where ds >= '{first_ds}' and ds <= '{date}' and platform_2 <> 'admin' and platform_2 <> 'admin_test'
             group by uid
        '''.format(date=date, first_ds=first_ds.strip(), )
        print pay_sql
        pay_df = hql_to_df(pay_sql)
        pay_df['num'] = 1
        print pay_df.head()
        result_df = info_df.merge(pay_df, on=['uid', ], how='left')
        result_df = result_df.groupby(['ds', 'vip_level', ]).agg(
            {'num': lambda g: g.sum(),'uid': lambda g: g.nunique(), 'order_money': lambda g: g.sum()}).reset_index().rename(columns={'num': 'pay_user_num',})

        result_df['date'] = date
        result_df['act_mon'] = act_mon
        print result_df.head()
        print '1'
        for a in result_df.values.tolist():
            res_list.append(a)

    pd.DataFrame(res_list).to_excel(r'/Users/kaiqigu/Documents/Superhero/超英-越南-活跃玩家的分月留存-付费-总_20180402.xlsx', index=False)


if __name__ == '__main__':

    for platform in ['superhero_vt',]:
        settings_dev.set_env(platform)
        data_reduce()
        # # result.to_csv(r'/Users/kaiqigu/Documents/Superhero/超英-越南-箱子数据_20180307.csv')
        # result.to_excel(r'/Users/kaiqigu/Documents/Superhero/superhero-bi-2018-pay-rank_20180320-4.xlsx', index=False,encoding='utf-8')
    print "end"
