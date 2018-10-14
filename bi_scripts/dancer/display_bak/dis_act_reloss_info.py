#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-10-12 下午2:55
@Author  : Andy
@File    : ew.py
@Software: PyCharm
Description :  武娘回流用户
'''
import settings_dev
from utils import hql_to_df, update_mysql, ds_add, date_range


def dis_act_reloss_info(date):
    table = 'dis_act_reloss_info'

    act_reloss_sql = '''
    select server,
       count(user_id) as act_sum,
       sum(case when vip=0 then 1 else 0 end) as vip0,
       sum(case when vip=1 then 1 else 0 end) as vip1,
       sum(case when vip=2 then 1 else 0 end) as vip2,
       sum(case when vip=3 then 1 else 0 end) as vip3,
       sum(case when vip=4 then 1 else 0 end) as vip4,
       sum(case when vip=5 then 1 else 0 end) as vip5,
       sum(case when vip=6 then 1 else 0 end) as vip6,
       sum(case when vip=7 then 1 else 0 end) as vip7,
       sum(case when vip=8 then 1 else 0 end) as vip8,
       sum(case when vip=9 then 1 else 0 end) as vip9,
       sum(case when vip=10 then 1 else 0 end) as vip10,
       sum(case when vip=11 then 1 else 0 end) as vip11,
       sum(case when vip=12 then 1 else 0 end) as vip12,
       sum(case when vip=13 then 1 else 0 end) as vip13,
       sum(case when vip=14 then 1 else 0 end) as vip14,
       sum(case when vip=15 then 1 else 0 end) as vip15
    from
    (
        select user_id,
               vip,
               reverse(substring(reverse(user_id), 8)) as server
        from   mid_info_all
        where  ds = '{date}'
    ) t1
    left semi join
    (
        select a.user_id from
        (
            select user_id
            from parse_info
            where  ds='{date}'
        ) a
        left outer join
        (
            select distinct user_id
            from parse_info
            where ds >= '{date_in_7days}' and ds <= '{date_in_1days}'
        ) b  on a.user_id = b.user_id
        where b.user_id is NULL
    ) t2 on t1.user_id = t2.user_id
    group by server
    '''.format(**{
        'date': date,
        'date_in_1days': ds_add(date, -1),
        'date_in_7days': ds_add(date, -7),
    })

    # print act_reloss_sql
    act_reloss_df = hql_to_df(act_reloss_sql)
    act_reloss_df['ds'] = date

    columns = ['ds', 'server', 'act_sum', 'vip0', 'vip1', 'vip2', 'vip3',
               'vip4', 'vip5', 'vip6', 'vip7', 'vip8', 'vip9', 'vip10',
               'vip11', 'vip12', 'vip13', 'vip14', 'vip15']
    act_reloss_df = act_reloss_df[columns]
    # print act_reloss_df
    # 更新MySQL表
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, act_reloss_df, del_sql)

    return act_reloss_df


if __name__ == '__main__':
    for platform in ('dancer_tw', 'dancer_pub'):
        settings_dev.set_env(platform)
        for date in date_range('20170119', '20170121'):
            dis_act_reloss_info(date)
