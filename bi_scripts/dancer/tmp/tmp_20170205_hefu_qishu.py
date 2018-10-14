#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 秦祺书合服脚本，自用。
'''
import settings_dev
from utils import hql_to_df
import pandas as pd

def tmp_20161128_hefushuju():

    #服务器信息
    server_sql = '''
    select t1.user_id, t1.ds, t1.server,t2.order_money,t2.pay_num from(
        select user_id,reverse(substr(reverse(user_id), 8)) as server, ds from parse_info  where ds>='20170223' and ds<='20170301' group by user_id, ds, server) t1
        left join
        (select
            user_id as pay_num,
            sum(order_money) as order_money,
            ds
        from raw_paylog
        where ds>='20170223' and ds<='20170301' and platform_2<>'admin_test' AND order_id not like '%testktwwn%' group by user_id, ds) t2
        on (t1.user_id = t2.pay_num and t1.ds=t2.ds)
    '''
    print server_sql
    server_df = hql_to_df(server_sql)
    print server_df.head(10)

    hefu_df = pd.read_excel(r'E:\Data\bi_scripts\dancer\server_list_tw.xlsx')
    # print hefu_df

    server_df = server_df.merge(hefu_df, on='server', how='left')


    server_df = server_df.groupby(['hefu', 'server', 'ds']).agg({
        'user_id': lambda g: g.count(),
        'pay_num': lambda g: g.nunique(),
        'order_money': lambda g: g.sum()
    }).reset_index()
    print server_df.head(10)

    server_df = server_df.groupby(['hefu', 'server']).agg({
        'user_id': lambda g: g.sum()*1.0/7,
        'pay_num': lambda g: g.sum()*1.0/7,
        'order_money': lambda g: g.sum()
    }).reset_index().rename(columns={
        'user_id': 'user_num',
        'order_money': 'server_money',
    })
    print server_df.head(10)

    #用户排名信息
    info_sql = '''
    select t1.user_id, t1.combat, t1.rn, t1.server, t2.money, t3.money7 from (
    select user_id, combat, rn, server from (
        select user_id, combat, row_number() over(partition by reverse(substr(reverse(user_id), 8)) order by combat desc) as rn,reverse(substr(reverse(user_id), 8)) as server
        from mid_info_all where ds='20170301') t3 where t3.rn<=15) t1
        left join ( select user_id, sum(order_money) as money from raw_paylog where ds<='20170301' and platform_2<>'admin_test' AND order_id not like '%testktwwn%' group by user_id
        ) t2 on t1.user_id = t2.user_id
        left join (select user_id, sum(order_money) as money7 from raw_paylog where ds<='20170301' and ds>='20170223' and platform_2<>'admin_test' AND order_id not like '%testktwwn%' group by user_id) t3
        on t1.user_id = t3.user_id
    '''
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head(10)

    result_df = info_df.merge(server_df, on='server', how='left').fillna(0)
    result_df['rank'] = result_df['combat'].groupby(result_df['hefu']).rank(ascending=False)

    return result_df

if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    result = tmp_20161128_hefushuju()
    result.to_excel(r'E:\Data\output\dancer\dancer_tw_hefu.xlsx')