#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 武娘多语言 - 分语言LTV
Time        : 2017.06.30
illustration:
'''
import pandas as pd
import settings_dev
from utils import hql_to_df
from utils import ds_add
from utils import update_mysql
from utils import format_dates
from utils import date_range
from utils import ds_delta
from dancer.cfg import LAN_TYPE

ltv_days = [1, 2, 3, 4, 5, 6, 7, 14, 30, 60, 90, 120, 150, 180]

def formatDate(Date, formatType='YYYY-MM-DD'):
    formatType = formatType.replace('YYYY', Date[0:4])
    formatType = formatType.replace('MM', Date[4:6])
    formatType = formatType.replace('DD', Date[-2:])
    return formatType

def dis_reg_user_ltv_lan_mul(date):
    start_date = ds_add(date, -max(ltv_days) + 1)  # 增加时间判断，11月25日起进行计算
    dates_to_update = [ds_add(date, -ltv_day + 1) for ltv_day in ltv_days]
    # dates_to_update = []
    # for date in date_range('20170629', date):
    #     dates_to_update.append(date)
    server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    f_date = formatDate(date)
    # 注册数据
    reg_sql = '''
        SELECT regexp_replace(to_date(regist_time),'-','') AS ds,
               account,
               register_lan_sort AS LANGUAGE
        FROM mid_info_all
        WHERE ds = '{date}'
        AND to_date(reg_time) <= '{f_date}'
        GROUP BY regexp_replace(to_date(regist_time),'-',''),
               account, register_lan_sort
        '''.format(date=date, f_date=f_date)
    print reg_sql
    reg_df = hql_to_df(reg_sql)
    # 所有的充值数据
    pay_sql = '''
    SELECT ds,
           account,
           sum(pay_rmb) AS pay_rmb
    FROM
      (SELECT ds,
              user_id,
              sum(order_money) AS pay_rmb
       FROM raw_paylog
       WHERE platform_2 != 'admin_test'
         AND ds >= '{start_date}'
         AND ds <= '{date}'
         AND order_id NOT LIKE '%test%'
       GROUP BY ds,
                user_id) pay
    JOIN
      (SELECT user_id,
              account
       FROM mid_info_all
       WHERE ds = '{date}') info ON info.user_id = pay.user_id
    GROUP BY ds,
             account
    '''.format(start_date=start_date, date=date)
    print pay_sql
    pay_df = hql_to_df(pay_sql)

    dfs = []
    for date_to_update in dates_to_update:
        days_list = []
        if date_to_update >= server_start_date:
            use_reg_df = reg_df[(reg_df.ds == date_to_update)].copy()
            ltv_dfs = use_reg_df.groupby(['ds', 'language']).agg(
                {'account': lambda g: g.nunique()}).reset_index().rename(
                    columns={
                        'account': 'reg_user_num'
                    })
            for ltv_ds in ltv_days:
                ltv_end_date = ds_add(date_to_update, ltv_ds - 1)
                if ltv_end_date <= date:
                    use_pay_df = pay_df[(pay_df.ds <= ltv_end_date) & (
                        pay_df.ds >= date_to_update)][['account', 'pay_rmb']]
                    use_pay_df['pay_num'] = use_pay_df['account']
                    result_ltv_df = use_reg_df.merge(use_pay_df,
                                                     on='account',
                                                     how='left')
                    result_ltv_df = result_ltv_df.groupby(
                        ['ds', 'language']).agg({
                            'account': lambda g: g.nunique(),
                            'pay_num': lambda g: g.nunique(),
                            'pay_rmb': lambda g: g.sum()
                        }).reset_index().rename(columns={
                            'account': 'reg_user_num',
                            'pay_num': 'd%s_pay_num' % ltv_ds,
                            'pay_rmb': 'd%s_pay_rmb' % ltv_ds
                        }).fillna(0)
                    result_ltv_df['d%s_ltv' % ltv_ds] = result_ltv_df[
                        'd%s_pay_rmb' %
                        ltv_ds] * 1.0 / result_ltv_df['reg_user_num']
                    ltv_dfs = ltv_dfs.merge(
                        result_ltv_df,
                        on=['ds', 'language', 'reg_user_num'])
                else:
                    days = ds_delta(date_to_update, ltv_end_date) + 1
                    days_list.append(days)
                    for ltv_date in ltv_days:
                        if ltv_date in days_list:
                            ltv_dfs['d%s_pay_num' % ltv_date] = 0
                            ltv_dfs['d%s_pay_rmb' % ltv_date] = 0
                            ltv_dfs['d%s_ltv' % ltv_date] = 0
            columns = sum((['d%d_pay_num' % ltv_day,
                            'd%d_ltv' % ltv_day, ] for ltv_day in ltv_days),
                          ['ds', 'reg_user_num', 'language'])
            ltv_dfs = ltv_dfs[columns]
            dfs.append(ltv_dfs)
        else:
            continue
    final_result = pd.concat(dfs)
    final_result = final_result.replace({'language': LAN_TYPE})
    # 更新MySQL表——每日LTV
    table = 'dis_language_ltv'
    del_sql = 'delete from {0} where ds in {1}'.format(
        table, format_dates(dates_to_update))
    update_mysql(table, final_result, del_sql)


if __name__ == '__main__':
    settings_dev.set_env('dancer_mul')
    date = '20170907'
    dis_reg_user_ltv_lan_mul(date)
