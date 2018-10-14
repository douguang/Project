#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-3-8 下午3:04
@Author  : Andy 
@File    : sanguo_weekly_report_two_weeks_dailys.py
@Software: PyCharm
Description :   机甲无双-周报-日常数据
'''

from utils import hql_to_df, ds_add, date_range
import settings_dev
import pandas as pd

def weekly_data(platform, date):
    if platform in ('dancer_pub', 'dancer_tw','sanguo_ks','sanguo_bt',):
        sql = '''
            select '{platform}' as platform, t1.ds, t1.dau, t2.dnu, t3.pay_num, t3.pay from(
            select count(user_id) as dau, ds from parse_info where ds>='{date_14}' and ds<='{date}' group by ds) t1 left join (
            select count(distinct user_id) as dnu, regexp_replace(substr(reg_time,1,10),'-','') as regtime from parse_info where ds>='{date_14}' and ds<='{date}' group by regtime) t2
             on t1.ds = t2.regtime left join (
            select count(user_id) as pay_num, sum(order_money) as pay, ds from raw_paylog where ds>='{date_14}' and ds<='{date}' and platform_2<>'admin_test' group by ds) t3 on t1.ds=t3.ds
        '''.format(date=date, date_14=ds_add(date, -13), platform=platform)
        print sql

    if platform in ('sanguo_tw', 'sanguo_tl'):
        sql = '''
            select '{platform}' as platform, t1.ds, t1.dau, t2.dnu, t3.pay_num, t3.pay from(
            select count(user_id) as dau, ds from parse_info where ds>='{date_14}' and ds<='{date}' group by ds) t1 left join (
            select count(distinct user_id) as dnu, regexp_replace(substr(reg_time,1,10),'-','') as regtime from parse_info where ds>='{date_14}' and ds<='{date}' group by regtime) t2
             on t1.ds = t2.regtime left join (
            select count(user_id) as pay_num, sum(order_money)/5 as pay, ds from raw_paylog where ds>='{date_14}' and ds<='{date}' and platform_2<>'admin_test' group by ds) t3 on t1.ds=t3.ds
        '''.format(date=date, date_14=ds_add(date, -13), platform=platform)
        print sql

    if platform in ('superhero_tw', 'superhero_pub'):
        # 超级英雄
        sql = '''
            select '{platform}' as platform, t1.ds, t1.dau, t2.dnu, t3.pay_num, t3.pay from(
            select count(uid) as dau, ds from raw_info where ds>='{date_14}' and ds<='{date}' group by ds) t1 left join (
            select count(distinct uid) as dnu, regexp_replace(substr(create_time,1,10),'-','') as regtime from raw_info where ds>='{date_14}' and ds<='{date}' group by regtime) t2
             on t1.ds = t2.regtime left join (
            select count(uid) as pay_num, sum(order_money) as pay, ds from raw_paylog where ds>='{date_14}' and ds<='{date}' and platform_2<>'admin_test' group by ds) t3 on t1.ds=t3.ds
            '''.format(date=date, date_14=ds_add(date, -13), platform=platform)
        print sql


    if platform in ('sanguo_ks', 'sanguo_tw', 'sanguo_tl','sanguo_bt',):
        sql = sql.replace('parse_info', 'raw_info')
    df = hql_to_df(sql)
    print df.head(10)


    week_list = []
    for week in ['this_week','last_week']:
        if week == 'this_week':
            start_week=ds_add(date, -6)
            end_week = date
        if week == 'last_week':
            start_week = ds_add(date, -13)
            end_week = ds_add(date, -7)
        rate =1
        if platform in ('sanguo_tw', 'sanguo_tl'):
            rate = 5
        arpu_arppu_pay_rate = '''
            select t1.platform,t1.week,t1.wau,t2.pay_num,t2.pay,(t2.pay_num/t1.wau) as pay_rate,(t2.pay/t1.wau) as arpu,(t2.pay/t2.pay_num)as arppu
            from (
                  select '{platform}' as platform,'{week}' as week,count(distinct user_id)as wau
                  from parse_actionlog
                  where ds>='{start_week}'
                  and ds<='{end_week}'
            )t1
            left outer join(
                  select '{week}' as week,count(distinct user_id)as pay_num,(sum(order_money)/{rate})as pay
                  from raw_paylog
                  where ds>='{start_week}'
                  and ds<='{end_week}'
                  and platform_2 !='admin_test'
            )t2 on t2.week=t1.week
            group by t1.platform,t1.week,t1.wau,t2.pay_num,t2.pay,arpu,arppu
        '''.format(week=week,start_week=start_week,end_week=end_week,platform=platform,rate=rate)
        print arpu_arppu_pay_rate

        we_df = hql_to_df(arpu_arppu_pay_rate)
        print df.head(10)
        week_list.append(we_df)

    week_df = pd.concat(week_list)

    return df,week_df

if __name__ == '__main__':
    date = '20171108'
    daily_date_14 = []
    arpu_arppu_pay_rate_list = []
    for platform in ['sanguo_tl','sanguo_bt','sanguo_ks',]:
        settings_dev.set_env(platform)
        daily_res,week_res = weekly_data(platform, date)
        daily_date_14.append(daily_res)
        arpu_arppu_pay_rate_list.append(week_res)
    result = pd.concat(daily_date_14)
    result.to_excel('/Users/kaiqigu/Documents/Sanguo/三国周报数据/week_report_two_week_daily_1.xlsx', index=False)

    result_2 = pd.concat(arpu_arppu_pay_rate_list)

    result_this = result_2[result_2['week']=='this_week']
    result_this = result_this.drop('week', 1)
    result_last = result_2[result_2['week']=='last_week']
    result_last = result_last.drop('week', 1)
    result_last = result_last.rename(
        columns={'wau': 'wau_l', 'wnu': 'wnu_l', 'pay_num': 'pay_num_l', 'pay': 'pay_l', 'pay_rate': 'pay_rate_l', 'arpu': 'arpu_l', 'arppu': 'arppu_l',})
    result_2 = pd.DataFrame(result_this).merge(result_last,on=['platform',])
    result_2['pay_rate_change'] = (result_2['pay_rate'] /result_2['pay_rate_l']) -1
    result_2['arpu_change'] = (result_2['arpu'] /result_2['arpu_l'])-1
    result_2['arppu_change'] = (result_2['arppu'] /result_2['arppu_l'])-1

    result_2.to_excel('/Users/kaiqigu/Documents/Sanguo/三国周报数据/week_report_two_week_daily_2.xlsx', index=False)
