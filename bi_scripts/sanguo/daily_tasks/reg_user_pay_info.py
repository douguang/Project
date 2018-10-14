#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 2017/9/26 0026 14:36
@Author  : Andy 
@File    : reg_user_pay_info.py
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
        select t1.user_id,t1.reg_ds,t2.ds,t2.order_money,t1.ip from (
            select user_id,regexp_replace(substring(reg_time,1,10),'-','') as reg_ds,ip from mid_info_all where ds='20170925' and regexp_replace(substring(reg_time,1,10),'-','') >= '20170922' group by user_id,reg_ds,ip
        )t1 left outer join(
            select ds,user_id,sum(order_money) as order_money from raw_paylog where ds>='20170922' and platform_2 != 'admin_test' group by ds,user_id
        )t2 on t1.user_id=t2.user_id
        group by t1.user_id,t1.reg_ds,t2.ds,t2.order_money,t1.ip
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
                yield [row.user_id,row.reg_ds, row.ds,row.order_money,row.ip,country]
            except:
                country = ''
                yield [row.user_id,row.reg_ds, row.ds,row.order_money,row.ip,country]
                pass

    result = pd.DataFrame(ip_lines(), columns=['user_id', 'reg_ds', 'ds', 'order_money','ip', 'country',])
    print result.head()


if __name__ == '__main__':
    settings_dev.set_env('sanguo_tl')
    a = data_reduce()
    a.to_excel('E:\sanguo_tl_reg_user_pay_info-20170926.xlsx', index=False)
    print "end"