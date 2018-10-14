#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 武娘国服分appid包LTV(韩鹏需求)，已完成，自用。
'''
from utils import hqls_to_dfs
from utils import ds_add
from utils import update_mysql
from utils import format_dates
from utils import date_range, ds_delta
import pandas as pd
import settings_dev

# ltv_days = [1, 2, 3, 7, 14]
# ltv_days = [7, 30]

ltv_days = range(1, 30)


def formatDate(Date, formatType='YYYY-MM-DD'):
    formatType = formatType.replace('YYYY', Date[0:4])
    formatType = formatType.replace('MM', Date[4:6])
    formatType = formatType.replace('DD', Date[-2:])
    return formatType


if __name__ == '__main__':
    # for platform in ['dancer_pub']:
    #     result = dis_reg_user_ltv('20170622', '20170625')
    #     result.to_excel(r'/Users/kaiqigu/Documents/Excel/appid_ltv.xlsx')
    settings_dev.set_env('dancer_pub')
    start_date = '20170630'
    date = '20170709'
    # def dis_reg_user_ltv(start_date, date):
    # start_date = ds_add(date, -max(ltv_days) + 1)
    # dates_to_update = [ds_add(start_date, ltv_day - 1) for ltv_day in ltv_days]
    dates_to_update = date_range(start_date, date)
    # server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    f_start_date = formatDate(start_date)
    f_date = formatDate(date)
    # 注册数据
    reg_sql = '''
    SELECT t1.ds,
           t1.account,
           t2.platform
    FROM
      (SELECT min(to_date(reg_time)) AS ds,
              account
       FROM mid_info_all
       WHERE ds = '{date}'
         AND to_date(reg_time) >= '{f_start_date}'
         AND to_date(reg_time) <= '{f_date}'
       GROUP BY account) t1
    JOIN
      ( SELECT account,
               platform
       FROM parse_actionlog
       WHERE ds >= '{start_date}'
         AND ds <= '{date}'
         AND platform = 'oppo'
       GROUP BY account,
                platform ) t2 ON t1.account = t2.account
    '''.format(date=date,
               start_date=start_date,
               f_start_date=f_start_date,
               f_date=f_date)
    # 所有的充值数据
    pay_sql = '''
    SELECT ds,
           account,
           pay_rmb
    FROM
      ( SELECT ds,
               user_id,
               sum(order_money) AS pay_rmb
       FROM raw_paylog
       WHERE platform_2 != 'admin_test'
         AND ds >= '{start_date}'
         AND ds <= '{date}'
         AND order_id NOT LIKE '%test%'
       GROUP BY ds,
                user_id ) pay
    LEFT JOIN
      ( SELECT user_id,
               account
       FROM mid_info_all
       WHERE ds = '{date}' ) info ON info.user_id = pay.user_id
    '''.format(start_date=start_date, date=date)
    print pay_sql, reg_sql
    reg_df, pay_df = hqls_to_dfs([reg_sql, pay_sql])
    reg_df['ds'] = reg_df.ds.str.replace('-', '')

    dfs = []
    for date_to_update in dates_to_update:
        days_list = []
        if date_to_update >= '20161110':
            use_reg_df = reg_df[(reg_df.ds == date_to_update)].copy()
            ltv_dfs = use_reg_df.groupby(['ds', 'platform']).agg(
                {'account': lambda g: g.nunique()}).reset_index().rename(
                    columns={
                        'account': 'reg_user_num'
                    })
            for ltv_ds in ltv_days:
                ltv_end_date = ds_add(date_to_update, ltv_ds - 1)
                if ltv_end_date <= date:
                    use_pay_df = pay_df[(pay_df.ds <= ltv_end_date) & (
                        pay_df.ds >= date_to_update)][['account', 'pay_rmb']]
                    use_pay_df['pay_num'] = 1
                    result_ltv_df = use_reg_df.merge(use_pay_df,
                                                     on='account',
                                                     how='left').fillna(0)
                    result_ltv_df = result_ltv_df.groupby(['ds', 'platform']).agg(
                        {
                            'account': lambda g: g.nunique(),
                            'pay_num': lambda g: g.sum(),
                            'pay_rmb': lambda g: g.sum()
                        }).reset_index().rename(columns={
                            'account': 'reg_user_num',
                            'pay_num': 'd%s_pay_num' % ltv_ds,
                            'pay_rmb': 'd%s_pay_rmb' % ltv_ds
                        })
                    result_ltv_df['d%s_ltv' % ltv_ds] = result_ltv_df[
                        'd%s_pay_rmb' %
                        ltv_ds] * 1.0 / result_ltv_df['reg_user_num']
                    ltv_dfs = ltv_dfs.merge(result_ltv_df,
                                            on=['ds', 'platform', 'reg_user_num'])
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
                          ['ds', 'reg_user_num', 'platform'])
            ltv_dfs = ltv_dfs[columns]
            dfs.append(ltv_dfs)
        else:
            continue
    final_result = pd.concat(dfs)
    print final_result.head(5)
    final_result.to_excel(r'/Users/kaiqigu/Documents/Excel/plat_ltv.xlsx')

    # return final_result

    # if __name__ == '__main__':
    #     for platform in ['dancer_pub']:
    #         settings_dev.set_env(platform)
    #         result = dis_reg_user_ltv('20170622', '20170625')
            # result.to_excel(r'/Users/kaiqigu/Documents/Excel/plat_ltv.xlsx')
