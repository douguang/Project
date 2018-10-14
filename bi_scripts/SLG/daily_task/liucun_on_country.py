#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: liucun_on_country.py 
@time: 18/1/23 上午7:05 
"""

from utils import hqls_to_dfs, get_rank, hql_to_df, date_range
import pandas as pd
from utils import ds_delta
import settings_dev
from pandas import DataFrame
import time
import datetime
from ipip import *
import sys
reload(sys)
sys.setdefaultencoding('utf8')


def data_reduce():
    demo_sql = '''
        select ds,uid,account,to_date(reg_time) as reg_ds,to_date(offline_time) as offline_ds from parse_info where ds>='20180119' group by ds,uid,account,reg_ds,offline_ds
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

    result_df = result_df[['uid','reg_ds','offline_ds','ip']]
    def card_evo_lines():
        for _, row in result_df.iterrows():
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

    result.to_excel(r'E:\星战帝国-多语言-reg_offline-国家-1_20180122-2.xlsx', index=False)

    plat_result = result.groupby(['reg_ds','country','delta']).agg({
        'uid': lambda g: g.nunique(),
    }).reset_index().rename(columns={'uid': 'uid_num',})

    plat_result.to_excel(r'E:\星战帝国-多语言-reg_offline-国家-2_20180122-2.xlsx', index=False)

if __name__ == '__main__':
    for platform in ['slg_mul', ]:
        settings_dev.set_env(platform)
        data_reduce()
    print "end"