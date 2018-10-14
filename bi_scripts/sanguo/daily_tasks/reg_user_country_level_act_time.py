#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 2017/8/24 0024 16:24
@Author  : Andy 
@File    : reg_user_country_level_act_time.py
@Software: PyCharm
Description :
'''

from ipip import *
import settings_dev
import pandas as pd
from utils import hql_to_df
from utils import ds_add
from utils import get_server_days,date_range

def data_reduce():
    info_sql = '''
         select regexp_replace(to_date(reg_time),'-','') AS ds,user_id,level,act_time,ip,language from mid_info_all where ds='20170907' and  regexp_replace(substring(reg_time,1,10),'-','') >='20170901' group by ds,user_id,level,act_time,ip,language
    '''
    print info_sql
    info_df = hql_to_df(info_sql).fillna(0.0)
    # info_df = info_df[info_df['ds']>'20170820']
    print info_df.head()

    info_df['ip'] = info_df['ip'].astype(basestring)
    IP.load(os.path.abspath("./tinyipdata_utf8.dat"))

    def ip_lines():
        for _, row in info_df.iterrows():
            ip = row.ip
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
                yield [row.ds,row.user_id,row.level, row.act_time,row.ip,country,row.language]
            except:
                country = ''
                yield [row.ds, row.user_id, row.level, row.act_time, row.ip, country, row.language]
                pass

    info_df = pd.DataFrame(ip_lines(), columns=['ds', 'user_id', 'level', 'act_time', 'ip', 'country','language'])
    print info_df.head()

    order_money_sql = '''
        select user_id,sum(order_money) as order_money from raw_paylog where ds>='20170901' and platform_2 != 'admin' and platform_2 != 'admin_test' group by user_id
    '''
    print order_money_sql
    order_money_df = hql_to_df(order_money_sql).fillna(0.0)
    # info_df = info_df[info_df['ds']>'20170820']
    print order_money_df.head()

    result = info_df.merge(order_money_df, on='user_id', how='left')
    return result

if __name__ == '__main__':
    settings_dev.set_env('sanguo_ks')
    a = data_reduce()
    a.to_excel('E:\sanguo_ks_reg_user_country_level_acttime-20170908-3.xlsx', index=False)
    print "end"
