#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 贡品抢夺
'''
import settings_dev
from utils import hql_to_df, update_mysql


def dis_gongping_rob(date):
    table = 'dis_gongping_rob'

    sql = '''
    select '{date}' as ds,
           vip,
           server,
           count(user_id) as dau_lv_gt_19,
           count(rob_times) as rob_user_num,
           sum(rob_times) as rob_times_all,
           -- rob_times_all/rob_user_num as avg_rob_times,
           sum(case when rob_times <= 10 then 1 else 0 end) as times_0_10,
           sum(case when rob_times >= 11 and rob_times <= 20 then 1 else 0 end) as times_11_20,
           sum(case when rob_times >= 21 and rob_times <= 30 then 1 else 0 end) as times_21_30,
           sum(case when rob_times >= 31 and rob_times <= 50 then 1 else 0 end) as times_31_50,
           sum(case when rob_times >= 51 and rob_times <= 70 then 1 else 0 end) as times_51_70,
           sum(case when rob_times >= 71 and rob_times <= 100 then 1 else 0 end) as times_71_100,
           sum(case when rob_times >= 101 and rob_times <= 140 then 1 else 0 end) as times_101_140,
           sum(case when rob_times >= 141 then 1 else 0 end) as times_ge_141
    from
    (
        select t2.user_id as user_id,
               rob_times,
               vip,
               server
        from
        (
            select split(body.a_usr, '@')[0] as user_id,
                   count(1) as rob_times
            from raw_actionlog
            where ds='{date}' and body.a_typ = '{method_name}'
            group by body.a_usr
        ) t1
        right outer join
        (
            select user_id,
                   vip,
                   reverse(substring(reverse(user_id), 8)) as server
            from mid_info_all
            where ds = '{date}' and level >= 19 and act_time >= '{start_act_time}'
        ) t2 on t1.user_id = t2.user_id
    ) t3
    group by vip, server
    '''.format(**{
        'date': date,
        'method_name': 'commander.rob',
        'start_act_time':
        date[0:4] + '-' + date[4:6] + '-' + date[6:8] + ' 00:00:00',
    })

    print sql
    df = hql_to_df(sql, 'hive')
    df['avg_rob_times'] = df['rob_times_all'] / df['rob_user_num']

    # import diskcache as dc
    # cache = dc.Cache('cache')
    # cache['df'] = df
    # print df
    # df = cache['df']

    # 更新 MySQL
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, df, del_sql)


if __name__ == '__main__':
    settings_dev.set_env('sanguo_ks')
    dis_gongping_rob('20160420')
