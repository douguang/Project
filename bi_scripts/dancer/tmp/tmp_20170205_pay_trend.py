#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description :  武娘国服 充值用户金额走势
例：10月份登录用户（不包含15日之后新增的用户）
未充值的用户，在11月份 仍未充值的有600人，充值0-500的有100人,充值501-2000的50人,充值2000以上的50人,流失玩家（月底7日未登录的）200人
Database    : dancer_tw
'''
import settings_dev
import pandas as pd
from utils import hql_to_df
def tmp_20170205_pay_trend():


    reg_sql = '''
        select user_id from mid_info_all where ds='20161115' and to_date(act_time)>='2016-11-10'
    '''
    reg_df = hql_to_df(reg_sql)
    # print reg_df

    pay_sql_11 = '''
        select
            t1.user_id,
            t1.pay,
            (case when t1.pay =0 then 0
            when t1.pay > 0 and  t1.pay <= 500 then 500
            when t1.pay > 500 and  t1.pay <= 2000 then 2000
            when t1.pay > 2000  then 3000 else 0 end) as dangwei
        from (
            select user_id,
            sum(order_money) as pay
            from raw_paylog
            where ds>='20161110' and ds<='20161130' and platform_2<>'admin_test'
            group by user_id) t1
    '''
    pay_df_11 = hql_to_df(pay_sql_11).rename(columns={'pay': 'pay_11', 'dangwei': 'dangwei_11'})


    pay_sql_12 = '''
        select
            t1.user_id,
            t1.pay,
            (case when t1.pay =0 then 0
            when t1.pay > 0 and  t1.pay <= 500 then 500
            when t1.pay > 500 and  t1.pay <= 2000 then 2000
            when t1.pay > 2000  then 3000 else 0 end) as dangwei
        from (
            select user_id,
            sum(order_money) as pay
            from raw_paylog
            where ds>='20161201' and ds<='20161231' and platform_2<>'admin_test'
            group by user_id) t1
    '''
    pay_df_12 = hql_to_df(pay_sql_12).rename(columns={'pay': 'pay_12', 'dangwei': 'dangwei_12'})
    # print pay_df_12

    liushi_sql = '''
        select user_id from parse_info where ds>='20161225' and ds<='20161231'
    '''
    liushi_df = hql_to_df(liushi_sql)

    data = reg_df.merge(pay_df_11, on='user_id', how='left').fillna(0)
    data = data.merge(pay_df_12, on='user_id', how='left').fillna(0)
    data['liushi'] = data['user_id'].isin(liushi_df['user_id'])
    data['liushi'] = data['liushi'].replace({True: 0, False: 1})
    data['dangwei_12'] = data['dangwei_12'] - data['liushi']
    data['dangwei_12'] = data['dangwei_12'].replace({499: -1, 1999: -1, 2999: -1})
    # print data

    result_df = data.groupby(['dangwei_11', 'dangwei_12']).user_id.count().reset_index().rename(columns={'user_id': 'pay_num'})
    pivot_df = pd.pivot_table(result_df, values='pay_num', index='dangwei_12', columns='dangwei_11', fill_value=0).reset_index()
    print pivot_df

    return pivot_df


if __name__ == '__main__':
    settings_dev.set_env('dancer_pub')
    result = tmp_20170205_pay_trend()
    result.to_excel('/home/kaiqigu/Documents/pay_trend_1.xlsx')
