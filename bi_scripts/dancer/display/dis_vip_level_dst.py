#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : vip等级分布
'''
from utils import hqls_to_dfs, hql_to_df
from utils import ds_add
from utils import update_mysql
from utils import date_range
import settings_dev


def dis_vip_level_dst(date):
    new_sql = '''
    SELECT '{date}' AS ds,
           vip_level,
           count(t1.user_id) AS new_vip_user,
           sum(pay) AS new_vip_pay_coin,
           count(DISTINCT t2.user_id) AS new_pay_user
    FROM
      (SELECT user_id,
              vip AS vip_level
       FROM parse_info
       WHERE ds = '{date}'
         AND vip > 0
         AND user_id NOT IN
           (SELECT user_id
            FROM mid_info_all
            WHERE ds = '{date_ago}'
              AND vip > 0)) t1
    LEFT OUTER JOIN
      (SELECT user_id,
              sum(order_money) AS pay
       FROM raw_paylog
       WHERE ds = '{date}'
         AND platform_2 != 'admin_test' AND order_id not like '%test%'
       GROUP BY user_id) t2 ON t1.user_id = t2.user_id
    GROUP BY vip_level
     '''.format(date_ago=ds_add(date, -1), date=date)
    all_sql = '''
    SELECT '{date}' AS ds,
           vip_level,
           sum(pay) AS income,
           count(t1.user_id) AS vip_user_total,
           count(t2.user_id) AS pay_user_total
    FROM
      (SELECT user_id,
              vip AS vip_level
       FROM parse_info
       WHERE ds = '{date}'
         AND vip > 0) t1
    LEFT OUTER JOIN
      (SELECT user_id,
              sum(order_money) AS pay
       FROM raw_paylog
       WHERE ds = '{date}'
         AND platform_2 != 'admin_test' AND order_id not like '%test%'
       GROUP BY user_id) t2 ON t1.user_id = t2.user_id
    GROUP BY vip_level
    '''.format(date=date)
    silence_sql = '''
        with a as
        (select user_id, vip as vip_level from mid_info_all where ds='{date}' and vip > 0 and regexp_replace(to_Date(act_time), '-', '') >= '{date_6}'),
        b as (select user_id, max(ds) as pay_date from raw_paylog where ds <= '{date}' group by user_id)
        select '{date}' AS ds, count(*) as silence_num, vip_level from a, b where a.user_id = b.user_id and b.pay_date>='{date_29}' and b.pay_date<'{date_6}' group by vip_level
    '''.format(date=date, date_6=ds_add(date, -6), date_29=ds_add(date, -29))
    # print silence_sql
    loss_sql = '''
        select '{date}' AS ds, count(distinct user_id) as lost_num, vip as vip_level from mid_info_all where ds='{date}' and regexp_replace(to_Date(act_time), '-', '') >= '{date_13}'
        and regexp_replace(to_Date(act_time), '-', '') < '{date_6}' group by vip_level
    '''.format(date=date, date_6=ds_add(date, -6), date_13=ds_add(date, -13))
    # print loss_sql

    # new_df, all_df = hqls_to_dfs([new_sql, all_sql])
    new_df = hql_to_df(new_sql)
    all_df = hql_to_df(all_sql)
    silence_df = hql_to_df(silence_sql)
    loss_df = hql_to_df(loss_sql)
    # 合并df
    ori_df = all_df.merge(new_df, on=['ds', 'vip_level'], how='left').merge(silence_df, on=['ds', 'vip_level'], how='left').merge(loss_df, on=['ds', 'vip_level'], how='left').fillna(0)
    ori_df['old_vip_user'] = ori_df['vip_user_total'] - ori_df['new_vip_user']
    ori_df['new_vip_login_rate'] = ori_df['new_vip_user'] / ori_df[
        'vip_user_total']
    ori_df['old_pay_user'] = ori_df['pay_user_total'] - ori_df['new_pay_user']
    ori_df['new_vip_pay_user_rate'] = ori_df['new_pay_user'] / ori_df[
        'pay_user_total']
    ori_df['old_vip_pay_coin'] = ori_df['income'] - ori_df['new_vip_pay_coin']
    ori_df['new_vip_pay_rate'] = ori_df['new_vip_pay_coin'] / ori_df['income']
    columns = ['ds', 'vip_level', 'vip_user_total', 'old_vip_user', 'new_vip_user', 'new_vip_login_rate', 'pay_user_total', 'old_pay_user', 'new_pay_user', 'new_vip_pay_user_rate',
               'income', 'old_vip_pay_coin', 'new_vip_pay_coin', 'new_vip_pay_rate', 'silence_num', 'lost_num']
    ori_df = ori_df[columns]
    result_df = ori_df.fillna(0)
    print result_df.head(5)
    # 更新MySQL表
    table = 'dis_vip_level_dst'
    print date, table
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)


if __name__ == '__main__':
    settings_dev.set_env('dancer_mul')
    # dis_vip_level_dst('20170802')
    for date in date_range('20170629', '20170905'):
        print date
        dis_vip_level_dst(date)
