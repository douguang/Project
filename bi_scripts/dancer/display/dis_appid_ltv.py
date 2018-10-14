#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 武娘国服分app包LTV，自用。
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


def dis_appid_ltv(date):

    start_date = ds_add(date, -max(ltv_days) + 1)  # 增加时间判断，11月25日起进行计算
    dates_to_update = [ds_add(date, -ltv_day + 1) for ltv_day in ltv_days]
    server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    f_start_date = formatDate(start_date)
    f_date = formatDate(date)

    # 注册数据
    reg_sql = '''
    select t1.account,
            t1.appid,
            t1.ds
    from
        (select
            account,
            appid,
            regexp_replace(to_date(reg_time), '-', '') as ds,
            row_number() over(partition by account order by regexp_replace(to_date(reg_time), '-', '')) as rn
        from
            mid_info_all
        where
            ds='{date}' and appid != '' and  regexp_replace(to_date(reg_time), '-', '')>='{start_date}') t1
    where t1.rn=1
    '''.format(date=date, start_date=start_date)
    # print reg_sql
    reg_df = hql_to_df(reg_sql)
    # print reg_df.head(5)
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
    JOIN
      ( SELECT user_id,
               account
       FROM mid_info_all
       WHERE ds = '{date}' ) info ON info.user_id = pay.user_id
    '''.format(start_date=start_date, date=date)
    # print pay_sql
    pay_df = hql_to_df(pay_sql)
    # print pay_df.head(5)

    dfs = []
    for date_to_update in dates_to_update:
        days_list = []
        if date_to_update >= '20170511' :
            use_reg_df = reg_df[(reg_df.ds == date_to_update)].copy()
            ltv_dfs = use_reg_df.groupby(['ds', 'appid']).agg(
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
                        ['ds', 'appid']).agg({
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
                    # print ltv_dfs.head(5)
                    # print result_ltv_df.head(5)
                    ltv_dfs = ltv_dfs.merge(
                        result_ltv_df,
                        on=['ds', 'appid', 'reg_user_num'])
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
                 for ltv_day in ltv_days), ['ds', 'reg_user_num', 'appid'])
            ltv_dfs = ltv_dfs[columns]
            dfs.append(ltv_dfs)
        else:
            continue
    final_result = pd.concat(dfs)
    # print final_result.head(5)
    # return final_result
    # 更新MySQL表——每日LTV
    table = 'dis_appid_ltv'
    del_sql = 'delete from {0} where ds in {1}'.format(
        table, format_dates(dates_to_update))
    update_mysql(table, final_result, del_sql)


if __name__ == '__main__':
    for platform in ['dancer_pub']:
        settings_dev.set_env(platform)
        for date in date_range('20170717', '20170717'):
            dis_appid_ltv(date)
        # result = dis_appid_ltv('20170108')
        # result.to_excel(r'E:\Data\output\dancer\appid_ltv.xlsx')
