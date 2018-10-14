#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 注册用户LTV(韩鹏需求)
'''
from utils import hql_to_df, ds_add, update_mysql, format_dates, date_range
import pandas as pd
import settings_dev

# ltv_days = [3, 7, 14, 30, 60]
ltv_days = range(1,61)
def formatDate(Date, formatType='YYYY-MM-DD'):
    formatType = formatType.replace('YYYY', Date[0:4])
    formatType = formatType.replace('MM', Date[4:6])
    formatType = formatType.replace('DD', Date[-2:])
    return formatType

def dis_reg_user_ltv(start_date, date):

    # start_date = ds_add(date, -max(ltv_days) + 1)
    # dates_to_update = [ds_add(start_date, ltv_day - 1) for ltv_day in ltv_days]
    dates_to_update = date_range(start_date,date)
    # server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    f_start_date = formatDate(start_date)
    f_date = formatDate(date)
    # 注册数据
    reg_sql = '''
    select min(to_date(reg_time)) as ds, account
    from mid_info_all
    where ds = '{date}'
      and to_date(reg_time) >= '{f_start_date}' and to_date(reg_time) <= '{f_date}' and account like '%ioskvgames_%'
    group by account
    '''.format(date=date, f_start_date=f_start_date, f_date=f_date)
    print reg_sql
    reg_df = hql_to_df(reg_sql)
    print reg_df.head(10)
    ziran_df = pd.read_excel(r'E:\Data\output\dancer\account_0307.xlsx')
    print ziran_df.head(10)
    reg_df = reg_df[reg_df['account'].isin(ziran_df['account'])]
    reg_df['ds'] = reg_df.ds.str.replace('-', '')
    print reg_df.head(10)
    # 所有的充值数据
    pay_sql = '''
    select ds,
           account,
           pay_rmb
    from
    (
        select ds,
               user_id,
               sum(order_money) as pay_rmb
        from raw_paylog
        where platform_2 != 'admin_test' and ds >= '{start_date}' and ds <= '{date}'
        AND order_id not like '%testktwwn%'
        group by ds, user_id
    ) pay
    join
    (
        select user_id, account
        from mid_info_all
        where ds = '{date}'
    ) info on info.user_id = pay.user_id
    '''.format(start_date=start_date, date=date)
    pay_df = hql_to_df(pay_sql)
    print pay_df.head(10)
    ziran_df = pd.read_excel(r'E:\Data\output\dancer\account_0307.xlsx')
    print ziran_df.head(10)
    pay_df = pay_df[pay_df['account'].isin(ziran_df['account'])]

    def yield_ltv():
        for date_to_update in dates_to_update:
            # if date_to_update < server_start_date:
            #     continue
            date_reg_accounts = reg_df[reg_df.ds == date_to_update]['account']
            reg_user_num = date_reg_accounts.count()
            print date_to_update, reg_user_num
            result_row = [date_to_update, reg_user_num]
            for ltv_day in ltv_days:
                ltv_end_date = ds_add(date_to_update, ltv_day - 1)
                if ltv_end_date > date:
                    pay_num = 0
                    ltv = 0
                else:
                    user_pay_df = pay_df[(pay_df.ds >= date_to_update) & (pay_df.ds <= ltv_end_date) & (pay_df.account.isin(date_reg_accounts))]
                    pay_num = user_pay_df.account.nunique()
                    if reg_user_num == 0:
                        ltv = 0
                    else:
                        ltv = user_pay_df.pay_rmb.sum()*1.0 / reg_user_num
                result_row.extend([pay_num, ltv])
            yield result_row

    columns = sum((['d%d_pay_num' % ltv_day, 'd%d_ltv' % ltv_day] for ltv_day in ltv_days), ['ds', 'reg_user_num'])
    result_df = pd.DataFrame(yield_ltv(), columns=columns)
    print result_df.head(10)
    return result_df


if __name__ == '__main__':
    for platform in ['dancer_pub']:
        settings_dev.set_env(platform)
        # for date in date_range('20160907', '20160920'):
        result = dis_reg_user_ltv('20170307', '20170320')
        result.to_excel(r'E:\Data\output\dancer\ziranliang_ltv.xlsx')
