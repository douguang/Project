#!/usr/bin/env python
# -- coding: UTF-8 --
'''
@Time : 2017/10/26 0026 11:40
@Author : Zhang Yongchen
@File : dis_platform_ltv.py.py
@Software: PyCharm Community Edition
Description :
'''

from utils import hql_to_df
from utils import ds_add
from utils import update_mysql
from utils import format_dates
from utils import date_range
from utils import ds_delta
import pandas as pd
import settings_dev


ltv_days = [1, 2, 3, 4, 5, 6, 7, 14, 30, 60, 90, 120, 150, 180]


def formatDate(Date, formatType='YYYY-MM-DD'):
    formatType = formatType.replace('YYYY', Date[0:4])
    formatType = formatType.replace('MM', Date[4:6])
    formatType = formatType.replace('DD', Date[-2:])
    return formatType


def dis_platform_ltv(date):

    start_date = ds_add(date, -max(ltv_days) + 1)  # 增加时间判断，11月25日起进行计算
    dates_to_update = [ds_add(date, -ltv_day + 1) for ltv_day in ltv_days]
    # dates_to_update = []
    # for date in date_range(start_date, date):
    #     dates_to_update.append(date)
    server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    f_start_date = formatDate(start_date)
    f_date = formatDate(date)

    # 注册数据
    reg_sql = '''
    SELECT account, plat, platform, t1.ds
    FROM (SELECT account, substr(uid, 1, 1) AS plat, row_number() OVER (PARTITION BY account ORDER BY create_time ASC) AS rn, platform_2 AS platform, regexp_replace(to_date(create_time), '-', '') AS ds
        FROM mid_info_all
        WHERE ds = '{date}'
            AND to_date(create_time) >= '{f_start_date}'
            AND to_date(create_time) <= '{f_date}'
        ) t1
    WHERE t1.rn = 1
    '''.format(date=date,
               f_start_date=f_start_date,
               f_date=f_date)
    print reg_sql
    reg_df = hql_to_df(reg_sql)
    # 所有的充值数据
    pay_sql = '''
    SELECT ds,
           account,
           pay_rmb
    FROM
      ( SELECT ds,
               uid,
               sum(order_money) AS pay_rmb
       FROM raw_paylog
       WHERE platform_2 != 'admin_test'
         AND ds >= '{start_date}'
         AND ds <= '{date}'
       GROUP BY ds,
                uid ) pay
    JOIN
      ( SELECT uid,
               account
       FROM mid_info_all
       WHERE ds = '{date}' ) info ON info.uid = pay.uid
    '''.format(start_date=start_date, date=date)
    print pay_sql
    pay_df = hql_to_df(pay_sql)
    # print pay_df.head(1)

    dfs = []
    for date_to_update in dates_to_update:
        days_list = []
        if date_to_update >= server_start_date:
            use_reg_df = reg_df[(reg_df.ds == date_to_update)].copy()
            ltv_dfs = use_reg_df.groupby(['ds', 'plat', 'platform']).agg(
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
                        ['ds', 'plat', 'platform']).agg({
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
                        on=['ds', 'plat', 'platform', 'reg_user_num'])
                    # print ltv_dfs.head(1)
                else:
                    days = ds_delta(date_to_update, ltv_end_date) + 1
                    days_list.append(days)
                    for ltv_date in ltv_days:
                        if ltv_date in days_list:
                            ltv_dfs['d%s_pay_num' % ltv_date] = 0
                            ltv_dfs['d%s_pay_rmb' % ltv_date] = 0
                            ltv_dfs['d%s_ltv' % ltv_date] = 0

                    # print ltv_dfs.head(5)
            columns = sum(
                (['d%d_pay_num' % ltv_day,
                  'd%d_ltv' % ltv_day, ]
                 for ltv_day in ltv_days), ['ds', 'reg_user_num', 'platform', 'plat'])
            ltv_dfs = ltv_dfs[columns]
            dfs.append(ltv_dfs)
        else:
            continue
    final_result = pd.concat(dfs)
    print final_result.head(5)
    # 更新MySQL表——每日LTV
    table = 'dis_platform_ltv'
    del_sql = 'delete from {0} where ds in {1}'.format(
        table, format_dates(dates_to_update))
    print del_sql
    column = ['ds', 'reg_user_num', 'platform'] + \
             ['d%d_pay_num' % day for day in ltv_days[:]] + \
             ['d%d_ltv' % day for day in ltv_days[:]]
    if settings_dev.code == 'superhero_bi':
        pub_result_df = final_result[final_result.plat == 'g'][column]
        ios_result_df = final_result[final_result.plat == 'a'][column]
        update_mysql(table, pub_result_df, del_sql, 'superhero_pub')
        update_mysql(table, ios_result_df, del_sql, 'superhero_ios')
    else:
        update_mysql(table, final_result[column], del_sql)

    print '{0} is complete'.format(table)
    return final_result


if __name__ == '__main__':
    for platform in ['superhero_bi']:
        settings_dev.set_env(platform)
        # for date in date_range('20171026', '20171031'):
        #     dis_platform_ltv(date)
        result = dis_platform_ltv('20171031')
        # result.to_excel(r'E:\Data\output\superhero\platform_ltv.xlsx')
