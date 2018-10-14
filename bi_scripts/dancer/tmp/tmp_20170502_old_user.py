#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 老玩家每日回归人数，次留，三留，回归玩家的充值人数，充值金额
老玩家回归，即四月1-17号未登陆且等级35级以上的玩家，在4月18号之后登陆即算回归。
Name        : tmp_20170502_homeland_task
Original    : tmp_20170502_homeland_task
'''
import settings_dev
import pandas as pd
import numpy as np
from utils import hql_to_df, get_config, ds_add


def tmp_20170502_old_user(date_old, date, date_now):

    old_user_sql = '''
        select
            t1.user_id,
            t1.ds,
            t2.start_date,
            nvl(t3.pay, 0) as pay
        from
            (select
                user_id,
                ds
            from
                parse_info
            where
                ds>='{date}' and
                user_id in
                    (select user_id from mid_info_all where ds='{date_now}'
                    and regexp_replace(to_date(act_time), '-', '')>='{date}'
                    and regexp_replace(to_date(reg_time), '-', '')<'{date_old}'
                    and level>=35 and user_id not in
                        (select user_id from mid_info_all where ds='{date_ago}' and
                        regexp_replace(to_date(act_time), '-', '') >='{date_old}'))) t1
        left join
            (select
                user_id,
                min(ds) as start_date
            from
                parse_info
            where
                ds>='{date}'
            group by
                user_id) t2
        on
            t1.user_id = t2.user_id
        left join
            (select
                user_id,
                sum(order_money) as pay,
                ds
            from
                raw_paylog
            where
                ds>='{date}' and
                platform_2<>'admin_test' and
                order_id not like '%test%'
            group by
                user_id, ds) t3
        on
            (t1.user_id=t3.user_id and t1.ds=t3.ds)
    '''.format(date=date, date_ago=ds_add(date, -1), date_old=date_old, date_now=date_now)
    print old_user_sql

    old_user_df = hql_to_df(old_user_sql)
    old_user_df['days'] = (pd.to_datetime(old_user_df['ds']) - pd.to_datetime(old_user_df['start_date'])).dt.days + 1

    # 留存
    liucun_df = pd.pivot_table(old_user_df, values='user_id', index='start_date', columns='days', aggfunc='count')
    liucun_df.to_excel(r'E:\Data\output\dancer\liucun.xlsx')
    # 充值
    pay_df = old_user_df[old_user_df['pay'] != 0]
    pay_df.to_excel(r'E:\Data\output\dancer\pay_detail.xlsx')
    pay_df['num'] = 1
    pay_df = pay_df.groupby('start_date').agg({'pay': lambda g : g.sum(), 'num': lambda g: g.sum()})
    pay_df.to_excel(r'E:\Data\output\dancer\pay.xlsx')


if __name__ == '__main__':
    for platform in ('dancer_pub',):
        settings_dev.set_env(platform)
        tmp_20170502_old_user('20170401', '20170414', '20170501')