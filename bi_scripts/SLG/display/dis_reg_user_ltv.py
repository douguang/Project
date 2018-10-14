#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 武娘多语言 - 注册用户LTV
Time        : 2017.06.30
illustration:
'''
from utils import hql_to_df
from utils import ds_add
from utils import update_mysql
from utils import format_dates
from utils import date_range
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
    # dates_to_update = []
    # for date in date_range('20170629', date):
    #     dates_to_update.append(date)
    server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    f_date = formatDate(date)
    # 注册数据
    reg_sql = '''
    SELECT regexp_replace(to_date(reg_time),'-','') AS ds,
           account
    FROM mid_info_all
    WHERE ds = '{date}'
    AND to_date(offline_time) <= '{f_date}'
    GROUP BY regexp_replace(to_date(reg_time),'-',''),
           account
    '''.format(date=date, f_date=f_date)
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
       WHERE platform != 'admin_test'
         AND ds >= '{start_date}'
         AND ds <= '{date}'
         AND order_type <> 1
         AND order_id NOT LIKE '%test%'
       GROUP BY ds,
                uid ) pay
    JOIN
      ( SELECT uid,
               account
       FROM mid_info_all
       WHERE ds = '{date}' ) info ON info.uid = pay.uid
    '''.format(start_date=start_date, date=date)
    pay_df = hql_to_df(pay_sql)

    def yield_ltv():
        for date_to_update in dates_to_update:
            if date_to_update < server_start_date:
                continue
            date_reg_accounts = reg_df[reg_df.ds == date_to_update]['account']
            if date_reg_accounts.__len__() == 0:
                continue
            reg_user_num = date_reg_accounts.count()
            # print date_to_update, reg_user_num
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
                    # print reg_user_num
                    ltv = user_pay_df.pay_rmb.sum() * 1.0 / reg_user_num
                result_row.extend([pay_num, ltv])
            yield result_row

    columns = sum((['d%d_pay_num' % ltv_day, 'd%d_ltv' % ltv_day]
                   for ltv_day in ltv_days), ['ds', 'reg_user_num'])
    result_df = pd.DataFrame(yield_ltv(), columns=columns)

    # 更新MySQL表——每日LTV
    table = 'dis_reg_user_ltv'
    del_sql = 'delete from {0} where ds in {1}'.format(
        table, format_dates(dates_to_update))
    update_mysql(table, result_df, del_sql)


if __name__ == '__main__':
    settings_dev.set_env('slg_mul')
    # date = '20170906'
    for date in date_range('20180119', '20180130'):
        print date
        try:
            dis_reg_user_ltv(date)
        except Exception, e:
            print e
