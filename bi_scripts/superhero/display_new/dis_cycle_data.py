#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 周期报表 - 每月数据
Time        : 2017.05.04
illustration:
'''
import datetime
import settings_dev
from utils import ds_add
from utils import hql_to_df
from utils import update_mysql
from lib.utils import check_week_month


def get_date(date, date_format_before='%Y%m%d'):
    # 返回指定日期的年月日
    year = datetime.datetime.strptime(date, date_format_before).year
    month = datetime.datetime.strptime(date, date_format_before).month
    day = datetime.datetime.strptime(date, date_format_before).day
    return year, month, day


if __name__ == '__main__':
    settings_dev.set_env('superhero_bi')
    date = '20170503'
    # 判断是否是月末 - 月度数据
    if check_week_month(date, 'm'):
        year, month, day = get_date(date)
        start_date = ds_add(date, -day + 1)
        assist_sql = '''
        SELECT plat,
            COUNT(user_id)/{day} AS avg_dau,
            SUM(order_money)/{day} AS avg_money,
            SUM(is_new_user)/{day} AS avg_new_user,
            SUM(case when order_money>0 then 1 else 0 end)/{day} AS avg_pay_num,
            SUM(order_money) AS sum_money,
            (SUM(case when order_money>0 then 1 else 0 end)/{day})/(COUNT(user_id)/{day}) AS avg_pay_rate,
            (SUM(order_money)/{day})/(COUNT(user_id)/{day}) AS avg_arpu,
            (SUM(order_money)/{day})/(SUM(case when order_money>0 then 1 else 0 end)/{day}) AS avg_arppu
        FROM mart_assist
        WHERE ds >= '{start_date}'
        AND ds <='{date}'
        GROUP BY plat
        '''.format(start_date=start_date,
                   date=date, day=day)
        result_df = hql_to_df(assist_sql)
        result_df['ds'] = date[:6]

        # 更新MySQL表
        table = 'dis_m_cycle_data'
        del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
        column = ['ds', 'avg_new_user', 'avg_dau', 'avg_pay_num', 'avg_money',
                  'avg_pay_rate', 'avg_arpu', 'avg_arppu', 'sum_money']

        if settings_dev.code == 'superhero_bi':
            pub_result_df = result_df[result_df.plat == 'g'][column]
            ios_result_df = result_df[result_df.plat == 'a'][column]
            update_mysql(table, pub_result_df, del_sql, 'superhero_pub')
            update_mysql(table, ios_result_df, del_sql, 'superhero_ios')
        else:
            update_mysql(table, result_df[column], del_sql)

        print '{0} is complete'.format(table)
    # 判断是否是周三 - 周数据
    if check_week_month(date, 'w'):
        start_date = ds_add(date, -6)
        day = 7
        assist_sql = '''
        SELECT plat,
            COUNT(user_id)/{day} AS avg_dau,
            SUM(order_money)/{day} AS avg_money,
            SUM(is_new_user)/{day} AS avg_new_user,
            SUM(case when order_money>0 then 1 else 0 end)/{day} AS avg_pay_num,
            SUM(order_money) AS sum_money,
            (SUM(case when order_money>0 then 1 else 0 end)/{day})/(COUNT(user_id)/{day}) AS avg_pay_rate,
            (SUM(order_money)/{day})/(COUNT(user_id)/{day}) AS avg_arpu,
            (SUM(order_money)/{day})/(SUM(case when order_money>0 then 1 else 0 end)/{day}) AS avg_arppu
        FROM mart_assist
        WHERE ds >= '{start_date}'
        AND ds <='{date}'
        GROUP BY plat
        '''.format(start_date=start_date,
                   date=date, day=day)
        result_df = hql_to_df(assist_sql)
        result_df = result_df.drop_duplicates()
        result_df['ds'] = date[:6]

        # 更新MySQL表
        table = 'dis_w_cycle_data'
        del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
        column = ['ds', 'avg_new_user', 'avg_dau', 'avg_pay_num', 'avg_money',
                  'avg_pay_rate', 'avg_arpu', 'avg_arppu', 'sum_money']

        if settings_dev.code == 'superhero_bi':
            pub_result_df = result_df[result_df.plat == 'g'][column]
            ios_result_df = result_df[result_df.plat == 'a'][column]
            update_mysql(table, pub_result_df, del_sql, 'superhero_pub')
            update_mysql(table, ios_result_df, del_sql, 'superhero_ios')
        else:
            update_mysql(table, result_df[column], del_sql)

        print '{0} is complete'.format(table)
