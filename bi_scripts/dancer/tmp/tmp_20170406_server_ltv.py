#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 武娘国服分渠道LTV(韩鹏需求)，已完成，自用。
'''
from utils import hql_to_df, ds_add, update_mysql, format_dates, date_range, ds_delta
import pandas as pd
import settings_dev

# ltv_days = [3, 7, 14, 30, 60]
ltv_days = range(1,61)
def formatDate(Date, formatType='YYYY-MM-DD'):
    formatType = formatType.replace('YYYY', Date[0:4])
    formatType = formatType.replace('MM', Date[4:6])
    formatType = formatType.replace('DD', Date[-2:])
    return formatType

def tmp_20170205_server_ltv(start_date, date):

    # start_date = ds_add(date, -max(ltv_days) + 1)
    # dates_to_update = [ds_add(start_date, ltv_day - 1) for ltv_day in ltv_days]
    dates_to_update = date_range(start_date,date)
    # server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    f_start_date = formatDate(start_date)
    f_date = formatDate(date)
    # 注册数据
    reg_sql = '''
    select to_date(reg_time) as ds, user_id,
    reverse(substr(reverse(user_id), 8)) as server
    from mid_info_all
    where ds = '{date}' and
      to_date(reg_time) >= '{f_start_date}' and to_date(reg_time) <= '{f_date}'
    '''.format(date=date, f_start_date=f_start_date, f_date=f_date, start_date=start_date)
    print reg_sql
    reg_df = hql_to_df(reg_sql)
    # print reg_df.head(10)
    reg_df['ds'] = reg_df.ds.str.replace('-', '')
    # print reg_df.head(1)
    # 所有的充值数据
    pay_sql = '''
        select ds,
               user_id,
               sum(order_money) as pay_rmb
        from raw_paylog
        where platform_2 != 'admin_test' and ds >= '{start_date}' and ds <= '{date}'
        and order_id not like '%testktwwn%'
        and user_id not in ( 'g139614071',
            'g136044371',
            'g133437322',
            'g131421665',
            'g137808305',
            'g133215447',
            'g133262140',
            'g134112097',
            'g134807633',
            'g135041448',
            'g258359127',
            'g252787709',
            'g34504848',
            'g259413841',
            'g257459856')
        group by ds, user_id
    '''.format(start_date=start_date, date=date)
    print pay_sql
    pay_df = hql_to_df(pay_sql)
    # print pay_df.head(1)

    dfs = []
    for date_to_update in dates_to_update:
        days_list = []
        if date_to_update >= '20161110':
            use_reg_df = reg_df[(reg_df.ds == date_to_update)].copy()
            ltv_dfs = use_reg_df.groupby(['ds', 'server']).agg({'user_id': lambda g: g.nunique()}).reset_index().rename(
                columns={
                    'user_id': 'reg_user_num'
                })
            for ltv_ds in ltv_days:
                ltv_end_date = ds_add(date_to_update, ltv_ds - 1)
                if ltv_end_date <= date:
                    use_pay_df = pay_df[(pay_df.ds <= ltv_end_date) & (
                        pay_df.ds >= date_to_update)][['user_id', 'pay_rmb']]
                    use_pay_df['pay_num'] = use_pay_df['user_id']
                    result_ltv_df = use_reg_df.merge(
                        use_pay_df, on='user_id', how='left')
                    result_ltv_df = result_ltv_df.groupby(['ds', 'server']).agg({
                        'user_id': lambda g: g.nunique(),
                        'pay_num': lambda g: g.nunique(),
                        'pay_rmb': lambda g: g.sum()
                    }).reset_index().rename(columns={
                        'user_id': 'reg_user_num',
                        'pay_num': 'd%s_pay_num' % ltv_ds,
                        'pay_rmb': 'd%s_pay_rmb' % ltv_ds
                    }).fillna(0)
                    result_ltv_df['d%s_ltv' % ltv_ds] = result_ltv_df['d%s_pay_rmb' % ltv_ds] * 1.0 / result_ltv_df[
                        'reg_user_num']
                    ltv_dfs = ltv_dfs.merge(
                        result_ltv_df, on=['ds', 'server', 'reg_user_num'])
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
            columns = sum((['d%d_pay_num' % ltv_day, 'd%d_ltv' % ltv_day, ] for ltv_day in ltv_days), ['ds', 'reg_user_num', 'server'])
            ltv_dfs = ltv_dfs[columns]
            dfs.append(ltv_dfs)
        else:
            continue
    final_result = pd.concat(dfs)
    print final_result.head(5)
    return final_result

if __name__ == '__main__':
    for platform in ['dancer_pub']:
        settings_dev.set_env(platform)
        # for date in date_range('20160907', '20160920'):
        result = tmp_20170205_server_ltv('20170301', '20170405')
        result.to_excel(r'E:\Data\output\dancer\server_ltv.xlsx')
