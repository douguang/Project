#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 分渠道LTV统计，排除自充
'''
from utils import date_range, ds_add, hql_to_df
import settings_dev
import pandas as pd


def tw_ltv(platform, date, end_date, ltvday):
    dates = date_range(date, end_date)
    ltv_days = range(1, ltvday+1)
    # 所有的充值数据
    pay_sql = '''
    select ds,
           user_id,
           sum(order_money) as pay_rmb
    from raw_paylog
    where ds >= '{date}'
      and platform_2 != 'admin_test'
    group by ds, user_id
    '''.format(date=date, end_date=end_date)
    pay_df = hql_to_df(pay_sql)

    # 注册数据
    reg_sql = '''
    select ds, user_id
    from raw_registeruser
    where ds >= '{date}'
      and ds <= '{end_date}'
    '''.format(date=date, end_date=end_date)

    reg_ltv_df = hql_to_df(reg_sql)
    print reg_ltv_df

    daily_ltv_dfs = []
    for date in dates:
        reg_daily_df = reg_ltv_df.loc[reg_ltv_df.ds == date].copy()
        for ltv_day in ltv_days:
            ltv_date = ds_add(date, ltv_day - 1)
            ltv_range_pay_df = pay_df.loc[(pay_df.ds >= date) & (pay_df.ds <= ltv_date)][['user_id', 'pay_rmb']].groupby('user_id').sum().reset_index()
            reg_daily_df = (reg_daily_df
                          .merge(ltv_range_pay_df, on='user_id', how='left')
                          .rename(columns={'pay_rmb': 'd%d_pay_rmb' % ltv_day})
                          .fillna(0)
                          )
        reg_daily_df['new_num'] = 1
        reg_daily_df = reg_daily_df.groupby(['ds','country']).sum()

        daily_ltv_dfs.append(reg_daily_df)

    result_df = pd.concat(daily_ltv_dfs).reset_index()
    for ltv_day in ltv_days:
        result_df['d%d_ltv' % ltv_day] = result_df['d%d_pay_rmb' % ltv_day] / result_df.new_num

    columns = ['ds','country','new_num'] + ['d%d_pay_rmb' % ltv_day for ltv_day in ltv_days] + ['d%d_ltv' % ltv_day for ltv_day in ltv_days]
    df = result_df[columns]
    print df
    df.to_excel(r'/home/kaiqigu/tmp_20160901_%s_country_tga_ltv.xlsx' % platform, index=False)
    return df

if __name__ == '__main__':
    for platform in ['dancer_pub']:
        settings_dev.set_env(platform)
        tw_ltv(platform, '20161110', '20161205', 30)