#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  流失用户最后打点
@software: PyCharm 
@file: liushi_user_last_action.py 
@time: 18/1/23 上午7:18 
"""

from utils import hqls_to_dfs, get_rank, hql_to_df, date_range
import pandas as pd
from utils import ds_delta
import settings_dev
from pandas import DataFrame
import time
import datetime
from ipip import *

def data_reduce():
    demo_sql = '''
        select ds,uid,to_date(reg_time) as reg_ds,to_date(offline_time) as offline_ds from parse_info where ds>='20180119' group by ds,uid,reg_ds,offline_ds
    '''
    print demo_sql
    demo_df = hql_to_df(demo_sql)
    print demo_df.head()

    nginx_sql = '''
            select account,ip from parse_nginx where ds> ='20180119' and ds<='20180120' and account != '' group by account,ip
        '''
    print nginx_sql
    nginx_df = hql_to_df(nginx_sql)
    print nginx_df.head()
    nginx_df = nginx_df.drop_duplicates(subset=['account', ], keep='first')
    result_df = demo_df.merge(nginx_df, on=['account', ], how='left')

    result_df['ip'] = result_df['ip'].astype(basestring)
    IP.load(os.path.abspath("./tinyipdata_utf8.dat"))

    demo_df = demo_df[['uid','reg_ds','offline_ds']]
    def card_evo_lines():
        for _, row in demo_df.iterrows():
            now = datetime.datetime.strptime(str(row.reg_ds), '%Y-%m-%d')
            end = datetime.datetime.strptime(str(row.offline_ds), '%Y-%m-%d')
            delta = ''
            if now <= end:
                delta = (end - now).days + 1
            else:
                delta = (now - end).days + 1
            # print [row.ds, row.server, row.reg_time,row.dau, delta]
            ip = row.ip
            country = ''
            try:
                country = IP.find(ip).strip().encode("utf8")
                if '中国台湾' in country:
                    country = '台湾'
                elif '中国香港' in country:
                    country = '香港'
                elif '中国澳门' in country:
                    country = '澳门'
                elif '中国' in country:
                    country = '中国'
            except:
                pass
            print [row.uid,row.reg_ds,row.offline_ds,row.ip,delta,country]
            yield [row.uid,row.reg_ds,row.offline_ds,row.ip,delta,country]

    result = pd.DataFrame(card_evo_lines(), columns=['uid', 'reg_ds', 'offline_ds','ip' ,'delta','country',])

    last_action_sql = '''
        select t1.ds,t1.a_usr as uid,t1.a_typ,t1.rn,to_date(t1.lt) as offline_ds from (
          select ds,a_usr,a_typ,row_number() over(partition by ds,a_usr order by lt desc) as rn,lt  from parse_actionlog where ds>='20180119' 
        )t1 
        where t1.rn=1 
    '''
    print last_action_sql
    action_df = hql_to_df(last_action_sql)
    print action_df.head()

    result = action_df.merge(result, on=['offline_ds', 'uid',], how='left')

    result.to_excel(r'/Users/kaiqigu/Documents/slg/星战帝国-多语言-reg_offline_last_action-国家-1_20180122-2.xlsx', index=False)

    plat_result = result.groupby(['reg_ds','country','delta','a_typ',]).agg({
        'uid': lambda g: g.nunique(),
    }).reset_index().rename(columns={'uid': 'uid_num',})

    plat_result.to_excel(r'/Users/kaiqigu/Documents/slg/星战帝国-多语言-reg_offline_last_action-国家-2_20180122-2.xlsx', index=False)

if __name__ == '__main__':
    for platform in ['slg_mul', ]:
        settings_dev.set_env(platform)
        data_reduce()
    print "end"