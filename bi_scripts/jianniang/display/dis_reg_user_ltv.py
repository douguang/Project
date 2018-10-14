#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 注册用户LTV
'''
from utils import hql_to_df, ds_add, update_mysql, format_dates, date_range
import pandas as pd
import settings_dev

ltv_days = [1, 2, 3, 4, 5, 6, 7, 14, 30, 60, 90, 120, 150, 180]


def formatDate(Date, formatType='YYYY-MM-DD'):
    formatType = formatType.replace('YYYY', Date[0:4])
    formatType = formatType.replace('MM', Date[4:6])
    formatType = formatType.replace('DD', Date[-2:])
    return formatType


def dis_reg_user_ltv(date):
    # 根据要求的LTV天数倒推需要的开始日期
    start_date = ds_add(date, -max(ltv_days) + 1)
    dates_to_update = [ds_add(date, -ltv_day + 1) for ltv_day in ltv_days]
    server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    f_start_date = formatDate(start_date)
    f_date = formatDate(date)
    # 注册数据
    reg_sql = '''
    SELECT regexp_replace(to_date(reg_time),'-','') AS ds,
           account
    FROM
      ( SELECT reg_time,
               account,
               row_number() over(partition BY account
                                 ORDER BY reg_time) AS rn
       FROM mid_info_all
       WHERE ds = '{date}'
         AND to_date(reg_time) <= '{f_date}') t1
    WHERE t1.rn=1
    '''.format(f_date=f_date, date=date)
    # print reg_sql
    reg_df = hql_to_df(reg_sql)
    # print reg_df.head(10)

    # 所有的充值数据
    pay_sql = '''
    SELECT ds,
           account,
           pay_rmb
    FROM
      ( SELECT ds,
               user_id,
               sum(order_rmb) AS pay_rmb
       FROM raw_paylog
       WHERE admin = 0
         AND ds >= '{start_date}'
         AND ds <= '{date}'
         AND status = 1
       GROUP BY ds,
                user_id ) pay
    JOIN
      ( SELECT user_id,
               account
       FROM mid_info_all
       WHERE ds = '{date}' ) info ON info.user_id = pay.user_id
    '''.format(start_date=start_date, date=date)
    # print pay_sql
    pay_df = hql_to_df(pay_sql)

    # print pay_df.head(10)

    def yield_ltv():
        for date_to_update in dates_to_update:
            if date_to_update < server_start_date:
                continue
            date_reg_accounts = reg_df[reg_df.ds == date_to_update]['account']
            if date_reg_accounts.__len__() == 0:
                continue
            reg_user_num = date_reg_accounts.count()
            result_row = [date_to_update, reg_user_num]
            for ltv_day in ltv_days:
                ltv_end_date = ds_add(date_to_update, ltv_day - 1)
                if ltv_end_date > date:
                    pay_num = 0
                    ltv = 0
                else:
                    user_pay_df = pay_df[(pay_df.ds >= date_to_update) & (
                        pay_df.ds <= ltv_end_date) & (pay_df.account.isin(
                            date_reg_accounts))]
                    pay_num = user_pay_df.account.nunique()
                    ltv = user_pay_df.pay_rmb.sum() * 1.0 / reg_user_num
                result_row.extend([pay_num, ltv])
            yield result_row

    columns = sum((['d%d_pay_num' % ltv_day, 'd%d_ltv' % ltv_day]
                   for ltv_day in ltv_days), ['ds', 'reg_user_num'])
    result_df = pd.DataFrame(yield_ltv(), columns=columns)
    # print result_df

    # 更新MySQL表——每日LTV
    table = 'dis_reg_user_ltv'
    del_sql = 'delete from {0} where ds in {1}'.format(
        table, format_dates(dates_to_update))
    update_mysql(table, result_df, del_sql)


if __name__ == '__main__':

    for platform in ['jianniang_tw']:
        settings_dev.set_env(platform)
        for date in date_range('20170708', '20170709'):
            # print date
            dis_reg_user_ltv(date)