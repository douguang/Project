#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 周报,vip,一次性跑完所有。
'''

from utils import hql_to_df, ds_add, date_range
import settings_dev
import time
import datetime
import pandas as pd


print '''周四执行'''


def formatDate(Date, formatType='YYYY-MM-DD'):
    formatType = formatType.replace('YYYY', Date[0:4])
    formatType = formatType.replace('MM', Date[4:6])
    formatType = formatType.replace('DD', Date[-2:])
    return formatType


def weekly_data_vip_num(platform, weekly_now, date_now):
    settings_dev.set_env(platform)

    # VIP人数
    vip_sql = '''
        SELECT '{platform}' AS platform,
               weekly_ago,
               vip_ago,
               weekly_now,
               vip_now
        FROM
          ( SELECT '{weekly_ago}' AS weekly_ago,
                  count(DISTINCT user_id) AS vip_ago
           FROM raw_info
           WHERE ds <= '{date_7ago}'
           and ds > '{date_14ago}'
           and vip>0
           ) t1
        JOIN
          ( SELECT '{weekly_now}' AS weekly_now,
                   count(distinct user_id) AS vip_now
           FROM raw_info
           WHERE ds <= '{date_now}'
           and ds > '{date_7ago}'
           and vip>0
           ) t2
        '''.format(**{
        'platform': platform,
        'weekly_now': weekly_now,
        'weekly_ago': weekly_now - 1,
        'date_7ago': ds_add(date_now, -7),
        'date_14ago': ds_add(date_now, -14),
        'date_now': date_now,
    })

    if platform in ('superhero_bi', 'superhero_vt'):
        vip_sql = vip_sql.replace('user_id', 'uid').replace('reg_time', 'create_time').replace('vip', 'vip_level')
    if platform in ('dancer_pub', 'dancer_tw', 'dancer_bt'):
        vip_sql = vip_sql.replace('raw_info', 'parse_info')
    vip_df = hql_to_df(vip_sql)
    print vip_df


def weekly_data_active_pay_num(platform, weekly_now, date_now):
    settings_dev.set_env(platform)
    # VIP活跃付费人数
    vip_active_sql = '''
        SELECT '{platform}' AS platform,
               weekly_ago,
               vip_ago,
               weekly_now,
               vip_now
        FROM
          ( SELECT '{weekly_ago}' AS weekly_ago,
                  count(DISTINCT user_id) AS vip_ago
           FROM raw_info
           WHERE ds <= '{date_7ago}'
           and ds > '{date_14ago}'
           and vip>0
           and user_id in (
           select user_id from raw_paylog where ds <= '{date_7ago}' and ds > '{date_14ago}' and platform_2<>'admin_test' and order_id not like '%testktwwn%' group by user_id
           )) t1
        JOIN
          ( SELECT '{weekly_now}' AS weekly_now,
                   count(distinct user_id) AS vip_now
           FROM raw_info
           WHERE ds <= '{date_now}'
           and ds > '{date_7ago}'
           and vip>0
           and user_id in (
           select user_id from raw_paylog where ds <= '{date_now}' and ds > '{date_7ago}' and platform_2<>'admin_test' and order_id not like '%testktwwn%' group by user_id
           )) t2
        '''.format(**{
        'platform': platform,
        'weekly_now': weekly_now,
        'weekly_ago': weekly_now - 1,
        'date_7ago': ds_add(date_now, -7),
        'date_14ago': ds_add(date_now, -14),
        'date_now': date_now,
    })

    if platform in ('superhero_bi', 'superhero_vt'):
        vip_active_sql = vip_active_sql.replace(
            'user_id', 'uid').replace('reg_time', 'create_time').replace('vip', 'vip_level')
    if platform in ('dancer_pub', 'dancer_tw', 'dancer_bt'):
        vip_active_sql = vip_active_sql.replace('raw_info', 'parse_info')
    vip_active_df = hql_to_df(vip_active_sql)
    print vip_active_df


def weekly_data_transform_num(platform, weekly_now, date_now):
    settings_dev.set_env(platform)
    # VIP转化率（转换人数）
    vip_transform_num_sql = '''
        SELECT '{platform}' AS platform,
               weekly_ago,
               vip_ago,
               weekly_now,
               vip_now
        FROM
          ( SELECT '{weekly_ago}' AS weekly_ago,
                  count(DISTINCT user_id) AS vip_ago
           FROM mid_info_all
           WHERE ds = '{date_7ago}' and regexp_replace(substr(reg_time,1,10),'-','') >'{date_14ago}' and regexp_replace(substr(reg_time,1,10),'-','') <='{date_7ago}'
           and vip>0
           ) t1
        JOIN
          ( SELECT '{weekly_now}' AS weekly_now,
                   count(distinct user_id) AS vip_now
           FROM mid_info_all
           WHERE ds = '{date_now}' and regexp_replace(substr(reg_time,1,10),'-','') >'{date_7ago}' and regexp_replace(substr(reg_time,1,10),'-','') <='{date_now}'
           and vip>0
           ) t2
        '''.format(**{
        'platform': platform,
        'weekly_now': weekly_now,
        'weekly_ago': weekly_now - 1,
        'date_7ago': ds_add(date_now, -7),
        'date_14ago': ds_add(date_now, -14),
        'date_now': date_now,
    })

    if platform in ('superhero_bi', 'superhero_vt'):
        vip_transform_num_sql = vip_transform_num_sql.replace(
            'user_id', 'uid').replace('reg_time', 'create_time').replace('vip', 'vip_level')
    if platform in ('dancer_pub', 'dancer_tw', 'dancer_bt'):
        vip_transform_num_sql = vip_transform_num_sql.replace('raw_info', 'parse_info')
    vip_transform_num_df = hql_to_df(vip_transform_num_sql)
    print vip_transform_num_df


def weekly_data_register_num(platform, weekly_now, date_now):
    settings_dev.set_env(platform)
    # VIP转化率（注册人数）
    reg_sql = '''
            SELECT '{platform}' AS platform,
                   weekly_ago,
                   vip_ago,
                   weekly_now,
                   vip_now
            FROM
              ( SELECT '{weekly_ago}' AS weekly_ago,
                      count(DISTINCT user_id) AS vip_ago
               FROM mid_info_all
               WHERE ds = '{date_7ago}' and regexp_replace(substr(reg_time,1,10),'-','') >'{date_14ago}' and regexp_replace(substr(reg_time,1,10),'-','') <='{date_7ago}'
               ) t1
            JOIN
              ( SELECT '{weekly_now}' AS weekly_now,
                       count(distinct user_id) AS vip_now
               FROM mid_info_all
               WHERE ds = '{date_now}' and regexp_replace(substr(reg_time,1,10),'-','') >'{date_7ago}' and regexp_replace(substr(reg_time,1,10),'-','') <='{date_now}'
               ) t2
            '''.format(**{
        'platform': platform,
        'weekly_now': weekly_now,
        'weekly_ago': weekly_now - 1,
        'date_7ago': ds_add(date_now, -7),
        'date_14ago': ds_add(date_now, -14),
        'date_now': date_now,
    })

    if platform in ('superhero_bi', 'superhero_vt'):
        reg_sql = reg_sql.replace(
            'user_id', 'uid').replace('reg_time', 'create_time').replace('vip', 'vip_level')
    if platform in ('dancer_pub', 'dancer_tw', 'dancer_bt'):
        reg_sql = reg_sql.replace('raw_info', 'parse_info')
    # print reg_sql
    reg_df = hql_to_df(reg_sql)
    print reg_df


def weekly_data_vip_new_num(platform, weekly_now, date_now):
    settings_dev.set_env(platform)
    # VIP新增VIP人数及付费
    vip_new_sql = '''
        SELECT '{platform}' AS platform,
               weekly_ago,
               money_ago,
               vip_ago
               weekly_now,
               money_now,
               vip_now
        FROM
          ( SELECT '{weekly_ago}' AS weekly_ago,
                  sum(order_money) as money_ago,
                  count(distinct user_id) as vip_ago
           FROM raw_paylog
           WHERE ds <= '{date_7ago}'
           and ds >= '{date_14ago}'
           and platform_2<>'admin_test'
           and order_id not like '%testktwwn%'
           and user_id in (
                  select user_id from mid_info_all where ds='{date_7ago}' and vip>0 and user_id not in (
                        select user_id from mid_info_all where ds = '{date_14ago}' and vip>0 group by user_id
           )group by user_id)) t1
        JOIN
          (SELECT '{weekly_now}' AS weekly_now,
                  sum(order_money) as money_now,
                  count(distinct user_id) as vip_now
           FROM raw_paylog
           WHERE ds <= '{date_now}'
           and ds >= '{date_7ago}'
           and platform_2<>'admin_test'
           and order_id not like '%testktwwn%'
           and user_id in (
                  select user_id from mid_info_all where ds='{date_now}' and vip>0 and user_id not in (
                        select user_id from mid_info_all where ds = '{date_7ago}' and vip>0 group by user_id
           )group by user_id)) t2
        '''.format(**{
        'platform': platform,
        'weekly_now': weekly_now,
        'weekly_ago': weekly_now - 1,
        'date_7ago': ds_add(date_now, -7),
        'date_14ago': ds_add(date_now, -14),
        'date_now': date_now,
    })

    if platform in ('superhero_bi', 'superhero_vt'):
        vip_new_sql = vip_new_sql.replace(
            'user_id', 'uid').replace('reg_time', 'create_time').replace('vip', 'vip_level')
    if platform in ('dancer_pub', 'dancer_tw', 'dancer_bt'):
        vip_new_sql = vip_new_sql.replace('raw_info', 'parse_info')
    vip_new_df = hql_to_df(vip_new_sql)
    print vip_new_df


if __name__ == '__main__':

    weekly_now = int(time.strftime('%W'))
    weeks_now = int(time.strftime('%w'))
    if weeks_now == 4:
        today = str(datetime.date.today()).replace('-', '')
    # weekly_now = 4
    # today = '20170126'
    date_now = ds_add(today, -1)
    print date_now

    print weekly_data_vip_num
    for platform in ['superhero_bi', 'superhero_vt', 'sanguo_ks', 'sanguo_bt', 'sanguo_tl', 'dancer_pub',  'dancer_tw', 'dancer_bt']:
        weekly_data_vip_num(platform, weekly_now, date_now)

    print weekly_data_active_pay_num
    for platform in ['superhero_bi', 'superhero_vt', 'sanguo_ks', 'sanguo_bt', 'sanguo_tl', 'dancer_pub',  'dancer_tw', 'dancer_bt']:
        weekly_data_active_pay_num(platform, weekly_now, date_now)

    print weekly_data_transform_num
    for platform in ['superhero_bi', 'superhero_vt', 'sanguo_ks', 'sanguo_bt', 'sanguo_tl', 'dancer_pub',  'dancer_tw', 'dancer_bt']:
        weekly_data_transform_num(platform, weekly_now, date_now)

    print weekly_data_register_num
    for platform in ['superhero_bi', 'superhero_vt', 'sanguo_ks', 'sanguo_bt', 'sanguo_tl', 'dancer_pub',  'dancer_tw', 'dancer_bt']:
        weekly_data_register_num(platform, weekly_now, date_now)
    #
    print weekly_data_vip_new_num
    for platform in ['superhero_bi', 'superhero_vt', 'sanguo_ks', 'sanguo_bt', 'sanguo_tl', 'dancer_pub',  'dancer_tw', 'dancer_bt']:
        weekly_data_vip_new_num(platform, weekly_now, date_now)