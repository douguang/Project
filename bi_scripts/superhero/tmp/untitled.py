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
           count(t1.uid) as dau,
           sum(case when new.uid is null then 0 else 1 end) as new,
           sum(case when new.uid is null or d2.uid is null then 0 else 1 end) as d2,
           sum(case when new.uid is null or d3.uid is null then 0 else 1 end) as d3,
           sum(case when new.uid is null or d7.uid is null then 0 else 1 end) as d7,
    from
    (
        select uid, coin
        from raw_info
        where reverse(substring(reverse(uid), 8)) = '{name}'
        and  ds = '{date}'
    ) t1
    left outer join
    (
        select uid
        from raw_reg
        where reverse(substring(reverse(uid), 8)) = '{name}'
        and ds = '{date}'
    ) new on t1.uid = new.uid
    left outer join
    (
        select uid
        from raw_activeuser
        where reverse(substring(reverse(uid), 8)) = '{name}'
        and  ds = '{d2}'
    ) d2 on t1.uid = d2.uid
    left outer join
    (
        select uid
        from raw_activeuser
        where reverse(substring(reverse(uid), 8)) = '{name}'
        and  ds = '{d3}'
    ) d3 on t1.uid = d3.uid
    left outer join
    (
        select uid
        from raw_activeuser
        where reverse(substring(reverse(uid), 8)) = '{name}'
        and  ds = '{d7}'
    ) d7 on t1.uid = d7.uid
    group by '{date}'
    '''.format(**{
        'date': date,
        'd2': ds_add(date, 2 - 1),
        'd3': ds_add(date, 3 - 1),
        'd7': ds_add(date, 7 - 1),
        'name': name
    })
    act_df = hql_to_df(act_hql)
    for i in [2,3,7]:
        act_df['d%s_rate' % i] = act_df['d%s' % i] / act_df['new']

    columns = ['ds',  'new', 'd2_rate', 'd3_rate','d7_rate']
    keep_rate_df = act_df[columns]


if __name__ == '__main__':
    settings.set_env('superhero_bi')
    date = '20160520'
    print date
    name ='g511'
    dis_act_keep_rate(date)
    # i = -6
    # while(i<0):
    #     date = ds_add(date,-1)
    #     i=i+1
    #     print date
    #     dis_act_keep_rate(date)
    #     continue
    # date = '20160426'
    # print ds_add(date,-13)
    # dis_act_keep_rate(ds_add(date,-13))
    # print ds_add(date,-29)
    # dis_act_keep_rate(ds_add(date,-29))
    # print ds_add(date,-59)
    # dis_act_keep_rate(ds_add(date,-59))
    # print ds_add(date,-89)
    # dis_act_keep_rate(ds_add(date,-89))

    act_hql = '''
    select {date} as ds,
           count(t1.uid) as dau,
           sum(case when new.uid is null then 0 else 1 end) as new,
           sum(case when new.uid is null or d2.uid is null then 0 else 1 end) as d2,
           sum(case when new.uid is null or d3.uid is null then 0 else 1 end) as d3,
           sum(case when new.uid is null or d7.uid is null then 0 else 1 end) as d7,
    from
    (
        select uid, coin
        from raw_info
        where reverse(substring(reverse(uid), 8)) = '{name}'
        and  ds = '{date}'
    ) t1
    left outer join
    (
        select uid
        from raw_reg
        where reverse(substring(reverse(uid), 8)) = '{name}'
        and ds = '{date}'
    ) new on t1.uid = new.uid
    left outer join
    (
        select uid
        from raw_info
        where reverse(substring(reverse(uid), 8)) = '{name}'
        and  ds = '{d2}'
    ) d2 on t1.uid = d2.uid
    left outer join
    (
        select uid
        from raw_info
        where reverse(substring(reverse(uid), 8)) = '{name}'
        and  ds = '{d3}'
    ) d3 on t1.uid = d3.uid
    left outer join
    (
        select uid
        from raw_info
        where reverse(substring(reverse(uid), 8)) = '{name}'
        and  ds = '{d7}'
    ) d7 on t1.uid = d7.uid
    group by '{date}'
    '''.format(**{
        'date': date,
        'd2': ds_add(date, 2 - 1),
        'd3': ds_add(date, 3 - 1),
        'd7': ds_add(date, 7 - 1),
        'name': name
    })
    act_df = hql_to_df(act_hql)
    for i in [2,3,7]:
        act_df['d%s_rate' % i] = act_df['d%s' % i] / act_df['new']

    columns = ['ds',  'new', 'd2_rate', 'd3_rate','d7_rate']
    keep_rate_df = act_df[columns]






