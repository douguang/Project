#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      :
Description :  - 注册用户LTV-分渠道（platform渠道用数字表示）
Time        : 20180814
illustration:
'''
from utils import hql_to_df
from utils import ds_add
from utils import update_mysql
from utils import format_dates
from utils import date_range
import pandas as pd
import settings_dev

ltv_days = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]

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
    SELECT regexp_replace(to_date(account_reg),'-','') AS ds,
           account
    FROM mid_info_all
    WHERE ds = '{date}'
    AND to_date(reg_time) <= '{f_date}'
    
    and platform= '35' 
    GROUP BY regexp_replace(to_date(account_reg),'-',''),
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
               user_id,
               sum(order_rmb) AS pay_rmb
       FROM raw_paylog
       WHERE platform != 'admin_test'
         AND ds >= '{start_date}'
         AND ds <= '{date}'
         AND order_id NOT LIKE '%test%'
       GROUP BY ds,
                user_id ) pay
    JOIN
      ( SELECT user_id,
               account
       FROM mid_info_all
       WHERE ds = '{date}'
        and platform= '35' ) info ON info.user_id = pay.user_id
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
    # table = 'dis_reg_user_ltv'
    # del_sql = 'delete from {0} where ds in {1}'.format(
    #     table, format_dates(dates_to_update))
    # update_mysql(table, result_df, del_sql)

    result_df.to_excel(r'C:\Users\Administrator\Desktop\chaoer-20180814-platform-35.xlsx', index=False, encoding='utf-8')

if __name__ == '__main__':
    for platform in ['superhero2', ]:
    #for platform in ('superhero2_tw','superhero2',):
        settings_dev.set_env(platform)
        # date = '20170906'
        for date in date_range('20180802', '20180814'):
            print date
            try:
                dis_reg_user_ltv(date)
            except Exception, e:
                print e
