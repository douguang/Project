#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: paylog_data.py 
@time: 18/1/22 上午10:57 
"""
from ipip import *
from utils import hqls_to_dfs, get_rank, hql_to_df, date_range
import pandas as pd
from utils import ds_delta
import settings_dev
from pandas import DataFrame
import time
import datetime

import sys
reload(sys)
sys.setdefaultencoding('utf8')

def data_reduce():
    pay_info_sql = '''
        select id,order_id,name,uid,order_money,server,goods_id,platform,order_time,order_type,order_des,money_type,device_pt,ds from raw_paylog where ds>='20180119'
    '''
    print pay_info_sql
    pay_df = hql_to_df(pay_info_sql)
    print pay_df.head()

    nginx_sql = '''
        select account,ip from parse_nginx where ds> ='20180119' and ds<='20180120' and account != '' group by account,ip
    '''
    print nginx_sql
    nginx_df = hql_to_df(nginx_sql)
    print nginx_df.head()

    info_sql = '''
        select account,uid from mid_info_all where ds='20180120' group by account,uid
    '''
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head()

    result_df = pay_df.merge(info_df, on=['uid',], how='left')
    print '--'
    print result_df
    result_df = result_df.merge(nginx_df, on=['account',], how='left')
    print result_df
    result_df['ip'] = result_df['ip'].astype(basestring)
    IP.load(os.path.abspath("./tinyipdata_utf8.dat"))

    def card_evo_lines():
        for _, row in result_df.iterrows():
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
            print [row.id,row.order_id,row.name,row.uid,row.order_money,row.server,row.goods_id,row.platform,row.order_time,row.order_type,row.order_des,row.money_type,row.device_pt,row.ds,row.account,row.ip,country]
            yield [row.id,row.order_id,row.name,row.uid,row.order_money,row.server,row.goods_id,row.platform,row.order_time,row.order_type,row.order_des,row.money_type,row.device_pt,row.ds,row.account,row.ip,country]

    result_df = pd.DataFrame(card_evo_lines(), columns=['id','order_id','name','uid','order_money','server','goods_id','platform','order_time','order_type','order_des','money_type','device_pt','ds','account','ip','country'])
    return result_df



if __name__ == '__main__':
    for platform in ['slg_mul', ]:
        settings_dev.set_env(platform)
        res = data_reduce()
        res.to_excel(r'E:\slg-mul_paylog_info_20180122.xlsx', index=False)
    print "end"