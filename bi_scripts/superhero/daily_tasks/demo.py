#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: demo.py 
@time: 18/2/28 下午6:12 
"""
import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range


def data_reduce():

    # info_sql = '''
    # select user_token as user_id, device from parse_nginx where ds>='20180206' and user_token <> '' and device <> ''  group by user_token, device
    # '''
    # print info_sql
    # info_df = hql_to_df(info_sql)
    # print info_df.head()

    data_sql = '''
        select t1.uid,t1.order_money,row_number() over(order by t1.order_money desc) as rn,t2.account,t2.nick,t2.create_time, t2.fresh_time, t2.zuanshi from (
          select uid,sum(order_money)as order_money  from raw_paylog where ds>='20171201' and platform_2 <> 'admin_test' group by uid
        )t1 left outer join(
          select uid,account,nick,create_time, fresh_time, zuanshi from mid_info_all where ds='20180227' group by uid,account,nick,create_time, fresh_time, zuanshi
          )t2 on t1.uid=t2.uid
        group by t1.uid,t1.order_money,t2.account,t2.nick,t2.create_time, t2.fresh_time, t2.zuanshi
    '''
    print data_sql
    data_df = hql_to_df(data_sql)
    data_df = data_df.head(100)
    print data_df.head()

    # res = data_df.merge(info_df, on=['user_id', ], how='left')vne45554551
    return data_df

if __name__ == '__main__':
    #dis_kaiqigu_pay_detail('20170102')
    for platform in ['superhero_vt',]:
        settings_dev.set_env(platform)
        result = data_reduce()
        result.to_csv(r'/Users/kaiqigu/Documents/Superhero/超英-越南-20171201以来充值前100的玩家_20180228-2.csv')
    print "end"