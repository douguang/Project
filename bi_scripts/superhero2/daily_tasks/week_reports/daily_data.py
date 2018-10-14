#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Author  : Dong Junshuang
@Software: Sublime Text
@Time    : 20170307
Description :  周报 - 数据概况
'''
from utils import hql_to_df
from utils import ds_add
import settings_dev


def get_daily_data(date, date_ago):

    daily_sql = '''
    SELECT act.ds,
           act.act_num,
           reg.reg_num,
           pay.sum_money
    FROM
      (SELECT ds,
              count(DISTINCT user_id) act_num
       FROM mid_info_all
       WHERE ds >='{date_ago}'
         AND ds <='{date}'
       GROUP BY ds) act
    JOIN
      (SELECT ds,
              count(DISTINCT user_id) reg_num
       FROM mid_info_all
       WHERE ds >='{date_ago}'
         AND ds <='{date}'
         AND regexp_replace(to_date(reg_time), "-", "")= ds
       GROUP BY ds) reg ON act.ds = reg.ds
    JOIN (
    SELECT ds,
           count(DISTINCT user_id) pay_num,
           sum(order_money) sum_money
    FROM raw_paylog
    WHERE ds >='{date_ago}'
         AND ds <='{date}'
      AND platform <> 'admin_test'
    GROUP BY ds) pay ON reg.ds = pay.ds
    ORDER BY act.ds
    '''.format(date=date, date_ago=date_ago)

    daily_df = hql_to_df(daily_sql)
    return daily_df


def week_daily_data(date, date_ago):
    week_sql = '''
    SELECT act.ds,
           pay.pay_num/act.wau as pay_rate,
           pay.sum_money/pay.pay_num as arppu,
           pay.sum_money/act.wau as arpu,
           act.wau,
           act.avg_dau,
           reg.reg_num,
           pay.pay_num,
           pay.sum_money
    FROM
      (SELECT '{date}' AS ds,
              count(DISTINCT user_id) wau,
              count(user_id)/7 avg_dau
       FROM raw_paylog
       WHERE ds >='{date_ago}'
         AND ds <='{date}'
       GROUP BY '{date}') act
    JOIN
      (SELECT '{date}' AS ds,
              count(DISTINCT user_id) reg_num
       FROM mid_info_all
       WHERE ds >='{date_ago}'
         AND ds <='{date}'
       GROUP BY '{date}') reg ON act.ds = reg.ds
    JOIN
      (SELECT '{date}' AS ds,
              count(DISTINCT user_id) pay_num,
              sum(order_money) sum_money
       FROM raw_paylog
       WHERE ds >='{date_ago}'
         AND ds <='{date}'
         AND platform <> 'admin_test'
       GROUP BY '{date}')pay ON reg.ds = pay.ds
    '''.format(date=date, date_ago=date_ago)

    # 查询本周的数据
    week_df = hql_to_df(week_sql)
    return week_df


if __name__ == '__main__':
    plat_list = ['superhero2_tw', ]
    for plat in plat_list:
        settings_dev.set_env(plat)
        # date = '20170301'
        for date in ['20180516']:
            print date
            date_ago = ds_add(date, -6)
            daily_df = get_daily_data(date, date_ago)
            week_df = week_daily_data(date, date_ago)
            daily_df.to_excel(
                r'/Users/kaiqigu/Desktop/BI_dancer/20/{plat}_daily_df_{date}.xlsx'.format(date=date,plat=plat))
            week_df.to_excel(
                r'/Users/kaiqigu/Desktop/BI_dancer/20/{plat}_daily_week_df_{date}.xlsx'.format(date=date,plat=plat))
