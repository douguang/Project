#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-7-18 下午4:04
@Author  : Andy 
@File    : dis_appid_ltv.py
@Software: PyCharm
Description :
'''

from utils import hql_to_df, ds_add, ds_delta, format_dates, date_range, update_mysql
import pandas as pd
import settings_dev

ltv_days = range(1,8)+[14,30,60,90,120,150,180]


def formatDate(Date, formatType='YYYY-MM-DD'):
    formatType = formatType.replace('YYYY', Date[0:4])
    formatType = formatType.replace('MM', Date[4:6])
    formatType = formatType.replace('DD', Date[-2:])
    return formatType


def dis_appid_ltv(date):
    # 根据要求的LTV天数倒推需要的开始日期
    start_date = ds_add(date, -max(ltv_days) + 1) # 增加时间判断，11月25日起进行计算
    dates_to_update = [ds_add(date, -ltv_day + 1) for ltv_day in ltv_days]
    server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    f_start_date = formatDate(start_date)
    f_date = formatDate(date)
    # 注册数据
    reg_ip_sql = '''
    select  account, bundle_id as appid,min(to_date(reg_time)) as ds
    from mid_info_all
    where ds = '{date}'
      and to_date(reg_time) >= '{f_start_date}' and to_date(reg_time) <= '{f_date}'
      group by account, bundle_id
    '''.format(date=date, f_start_date=f_start_date, f_date=f_date)
    reg_df = hql_to_df(reg_ip_sql)
    reg_df['ds'] = reg_df.ds.str.replace('-', '')


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
        where platform_2 != 'admin_test' and platform_2 != 'test' and ds >= '{start_date}' and ds <= '{date}' and order_id not like '%testktwwn%'
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
    # LTV计算主体
    dfs = []
    for date_to_update in dates_to_update:
        days_list = []
        if date_to_update >= server_start_date:
            use_reg_df = reg_df[(reg_df.ds == date_to_update)].copy()
            ltv_dfs = use_reg_df.groupby(['ds', 'appid']).agg({'account': lambda g: g.nunique()}).reset_index().rename(columns={
                        'account': 'reg_user_num'
                        })
            for ltv_ds in ltv_days:
                ltv_end_date = ds_add(date_to_update, ltv_ds - 1)
                if ltv_end_date <= date:
                    use_pay_df = pay_df[(pay_df.ds <= ltv_end_date) & (pay_df.ds >= date_to_update)][['account', 'pay_rmb']]
                    use_pay_df['pay_num'] = 1
                    result_ltv_df = use_reg_df.merge(use_pay_df, on='account', how='left').fillna(0)
                    result_ltv_df = result_ltv_df.groupby(['ds', 'appid']).agg({
                        'account': lambda g: g.nunique(),
                        'pay_num': lambda g: g.sum(),
                        'pay_rmb': lambda g: g.sum()
                    }).reset_index().rename(columns={
                        'account': 'reg_user_num',
                        'pay_num': 'd%s_pay_num' % ltv_ds,
                        'pay_rmb': 'd%s_pay_rmb' % ltv_ds
                        })
                    result_ltv_df['d%s_ltv' % ltv_ds] = result_ltv_df['d%s_pay_rmb' % ltv_ds] / result_ltv_df['reg_user_num']
                    ltv_dfs = ltv_dfs.merge(result_ltv_df, on=['ds', 'appid', 'reg_user_num'])
                else:
                    days = ds_delta(date_to_update, ltv_end_date) + 1
                    days_list.append(days)
                    for ltv_date in ltv_days:
                        if ltv_date in days_list:
                            ltv_dfs['d%s_pay_num' % ltv_date] = 0
                            ltv_dfs['d%s_ltv' % ltv_date] = 0
            # print ltv_dfs
            columns = sum((['d%d_pay_num' % ltv_day, 'd%d_ltv' % ltv_day]
                           for ltv_day in ltv_days), ['ds', 'reg_user_num', 'appid'])
            ltv_dfs = ltv_dfs[columns]
            dfs.append(ltv_dfs)
        else:
            continue
    final_result = pd.concat(dfs)
    print final_result
    # 更新MySQL表——每日LTV
    table = 'dis_appid_ltv'
    del_sql = 'delete from {0} where ds in {1}'.format(
        table, format_dates(dates_to_update))
    update_mysql(table, final_result, del_sql)

if __name__ == '__main__':
    for platform in ['sanguo_tl']:
        settings_dev.set_env(platform)
        # date = '20161120'
        for date in date_range('20170702', '20170727'):
            dis_appid_ltv(date)