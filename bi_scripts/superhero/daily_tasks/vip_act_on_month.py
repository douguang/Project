#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site: 分月VIP数据统计
@software: PyCharm 
@file: vip_act_on_month.py 
@time: 18/4/2 下午5:51 
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
    for reg_mon in ['201711', '201712', '201801', '201802', '201803']:
        for date in ['20171130','20171231','20180131','20180228','20180331']:
            act_mon = ''
            if date == '20171130':
                act_mon = '201711'
            if date == '20171231':
                act_mon = '201712'
            if date == '20180131':
                act_mon = '201801'
            if date == '20180228':
                act_mon = '201802'
            if date == '20180331':
                act_mon = '201803'
            info_sql = '''
              select ds,vip_level, count(distinct uid) from mid_info_all where ds = '{date}' and substr(regexp_replace(to_date(create_time),'-',''),1,6) = '{reg_mon}' and substr(regexp_replace(to_date(fresh_time),'-',''),1,6)  = '{act_mon}'
              group by ds,vip_level
            '''.format(date=date,reg_mon=reg_mon,act_mon=act_mon,)
            print info_sql
            info_df = hql_to_df(info_sql)
            info_df['date'] = date
            info_df['reg_mon'] = reg_mon
            info_df['act_mon'] = act_mon
            for a in info_df.values.tolist():
                res_list.append(a)

    pd.DataFrame(res_list).to_excel(r'/Users/kaiqigu/Documents/Superhero/超英-越南-活跃玩家的分月留存2_20180402.xlsx', index=False)


if __name__ == '__main__':

    for platform in ['superhero_vt',]:
        settings_dev.set_env(platform)
        data_reduce()
        # # result.to_csv(r'/Users/kaiqigu/Documents/Superhero/超英-越南-箱子数据_20180307.csv')
        # result.to_excel(r'/Users/kaiqigu/Documents/Superhero/superhero-bi-2018-pay-rank_20180320-4.xlsx', index=False,encoding='utf-8')
    print "end"
