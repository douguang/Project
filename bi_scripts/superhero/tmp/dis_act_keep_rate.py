#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 三国用户留存率
'''
import settings
import pandas as pd
from utils import ds_add, hql_to_df, update_mysql, get_config, hqls_to_dfs

def dis_act_keep_rate(date):
    act_hql = '''
    select {date} as ds,
           sum(case when new.user_id is null or d2.user_id is null then 0 else 1 end) as d2,
           sum(case when new.user_id is null or d3.user_id is null then 0 else 1 end) as d3,
           sum(case when new.user_id is null or d7.user_id is null then 0 else 1 end) as d7,
    from
    (
        select user_id, coin
        from raw_info
        where ds = '{date}'
    ) t1
    left outer join
    (
        select user_id
        from raw_reg
        where ds = '{date}'
    ) new on t1.user_id = new.user_id
    left outer join
    (
        select user_id
        from raw_activeuser
        where ds = '{d2}'
    ) d2 on t1.user_id = d2.user_id
    left outer join
    (
        select user_id
        from raw_activeuser
        where ds = '{d3}'
    ) d3 on t1.user_id = d3.user_id
    (
        select user_id
        from raw_activeuser
        where ds = '{d7}'
    ) d7 on t1.user_id = d7.user_id
    group by '{date}'
    '''.format(**{
        'date': date,
        'd2': ds_add(date, 2 - 1),
        'd3': ds_add(date, 3 - 1),
        'd7': ds_add(date, 7 - 1),
    })
    keep_rate_df = hql_to_df(act_hql)

    print keep_rate_df
    return keep_rate_df

if __name__ == '__main__':
    settings.set_env('superhero_bi')
    date = '20160410'
    print date
    dis_act_keep_rate(date)
    i = -6
    while(i<0):
    	date = ds_add(date,-1)
    	i=i+1
    	print date
    	dis_act_keep_rate(date)
    	continue
    date = '20160426'
    print ds_add(date,-13)
    dis_act_keep_rate(ds_add(date,-13))
    print ds_add(date,-29)
    dis_act_keep_rate(ds_add(date,-29))
    print ds_add(date,-59)
    dis_act_keep_rate(ds_add(date,-59))
    print ds_add(date,-89)
    dis_act_keep_rate(ds_add(date,-89))







