#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description :武娘台服 储值档位分析 秦祺书
'''
# from utils import hql_to_df, ds_add, hqls_to_dfs
import settings_dev
import utils

from pandas import DataFrame

def tmp_20170109_chuzhi():

    sql = '''
        select 
            t1.user_id,
            t1.ds,
            t1.server,
            t1.pay,
            (case when t1.pay <50 then 1
            when t1.pay >= 50 and  t1.pay < 150 then 2
            when t1.pay >= 150 and  t1.pay < 300 then 3
            when t1.pay >= 300 and  t1.pay < 500 then 4
            when t1.pay >= 500 and  t1.pay < 1000 then 5
            when t1.pay >= 1000 and  t1.pay < 2000 then 6
            when t1.pay >= 2000 and  t1.pay < 3000 then 7
            when t1.pay >= 3000 and  t1.pay < 5000 then 8
            when t1.pay >= 5000 and  t1.pay < 7500 then 9
            when t1.pay >= 7500 and  t1.pay < 10000 then 10
            when t1.pay >= 10000 then 11 else 0 end) as dangwei
        from (
            select user_id, ds, reverse(substr(reverse(user_id),8)) as server, sum(order_money) as pay from raw_paylog where ds>='20170106' and platform_2<>'admin_test' group by ds,user_id
        ) t1
    '''
    print sql
    df = utils.hql_to_df(sql).fillna(0)
    print df.head(10)

    return df

if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    result = tmp_20170109_chuzhi()
    print result.head(10)
    result.to_excel('/home/kaiqigu/Documents/chuzhi.xlsx')


