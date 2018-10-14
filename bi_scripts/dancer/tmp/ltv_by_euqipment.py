#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-7 上午9:36
@Author  : Andy 
@File    : ltv_by_euqipment.py
@Software: PyCharm
Description :  武娘  分渠道LTV
'''

from utils import date_range, ds_add, hql_to_df
import settings_dev
import pandas as pd

def tw_ltv(platform, date, end_date, ltvday):
    dates = date_range(date, end_date)
    ltv_days = range(1, ltvday + 1)
    # 所有的充值数据
    pay_sql = '''
    select ds,
           user_id,
           sum(order_money) as pay_rmb
    from raw_paylog
    where ds >= '{date}'
      and platform_2 != 'admin_test'
      and order_id not like '%testktwwn%'
    group by ds, user_id
    '''.format(date=date, end_date=end_date)
    print pay_sql
    pay_df = hql_to_df(pay_sql)

    equipment_sql = '''
            select user_id,platform as country
            from parse_actionlog
            where ds >= '{date}'
            and ds <= '{end_date}'
            group by user_id,platform
        '''.format(date=date, end_date=end_date)
    print equipment_sql
    vt_df = hql_to_df(equipment_sql)
    print vt_df

    reg_sql = '''
          select regexp_replace(substr(reg_time,1,10),'-','') as ds,user_id
          from parse_info
          where ds >= '{date}'
           and ds <= '{end_date}'
          group by regexp_replace(substr(reg_time,1,10),'-',''),user_id
    '''.format(date=date, end_date=end_date)
    reg_df = hql_to_df(reg_sql)
    reg_ltv_df = vt_df.merge(reg_df, on='user_id', how='inner')

    print reg_ltv_df
    daily_ltv_dfs = []
    for date in dates:
        reg_daily_df = reg_ltv_df.loc[reg_ltv_df.ds == date].copy()
        for ltv_day in ltv_days:
            ltv_date = ds_add(date, ltv_day - 1)
            ltv_range_pay_df = pay_df.loc[(pay_df.ds >= date) & (pay_df.ds <= ltv_date)][
                ['user_id', 'pay_rmb']].groupby('user_id').sum().reset_index()
            reg_daily_df = (reg_daily_df
                            .merge(ltv_range_pay_df, on='user_id', how='left')
                            .rename(columns={'pay_rmb': 'd%d_pay_rmb' % ltv_day})
                            .fillna(0)
                            )
        reg_daily_df['new_num'] = 1
        reg_daily_df = reg_daily_df.groupby(['ds', 'country']).sum()

        daily_ltv_dfs.append(reg_daily_df)

    result_df = pd.concat(daily_ltv_dfs).reset_index()
    for ltv_day in ltv_days:
        result_df['d%d_ltv' % ltv_day] = result_df[
            'd%d_pay_rmb' % ltv_day] / result_df.new_num

    columns = ['ds', 'country', 'new_num'] + ['d%d_pay_rmb' %
                                              ltv_day for ltv_day in ltv_days] + ['d%d_ltv' % ltv_day for ltv_day in ltv_days]
    df = result_df[columns]
    print df
    df.to_excel(r'/home/kaiqigu/桌面/武娘_1110-1206_分渠道_LTV.xlsx', index=False)
    return df
if __name__ == '__main__':
    for platform in ['dancer_pub']:
        settings_dev.set_env(platform)
        tw_ltv(platform, '20161110', '20161206', 30)
    print "end"



