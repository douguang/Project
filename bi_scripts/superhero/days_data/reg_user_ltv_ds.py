#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 注册用户ltv(account)(指定日期区间)(批量补数据)
注：需手动删除更新的数据
'''
from utils import hqls_to_dfs
from utils import ds_add
from utils import update_mysql
from utils import date_range
import settings_dev
import pandas as pd
from pandas import DataFrame


if __name__ == '__main__':
    settings_dev.set_env('superhero_tw')
    plat = 'superhero_tw'
    start_date = '20170112'
    end_date = '20170523'
    final_date = '20170523'
    date = start_date
    # Ltv日期
    ltv_day = [1, 3, 7, 14, 30, 60, 90, 120, 150, 180]

    # 排除开服至今的gs数据
    pay_sql = '''
    SELECT a.ds,
           b.account,
           a.order_money
    FROM
      (SELECT ds,
              uid,
              order_money
       FROM raw_paylog
       WHERE ds >= '{date}' )a
    JOIN
      ( SELECT uid,
               account
       FROM mid_new_account
       WHERE ds >= '{date}'
         AND ds <= '{end_date}'
         -- and plat = 'a'
         AND UID NOT IN (SELECT DISTINCT uid FROM mid_gs)
         )b ON a.uid = b.uid
    '''.format(date=date, end_date=end_date)
    reg_sql = '''
    SELECT distinct ds,
           account
    FROM mid_new_account
    where ds >= '{0}'
    and ds <= '{1}'
    -- and plat = 'a'
    AND UID NOT IN (SELECT DISTINCT uid FROM mid_gs)
    '''.format(date, end_date)
    pay_df, reg_df = hqls_to_dfs([pay_sql, reg_sql])

    date_list = date_range(start_date, end_date)

    dfs = []
    for date in date_list:
        print date
        reg_data = reg_df.loc[reg_df.ds == date]
        reg_num = reg_data.account.nunique()
        data = {}
        data['ds'] = [date]
        data['reg_user_num'] = [reg_num]
        for i in ltv_day:
            ltv_days = [ds_add(date, dt) for dt in range(0, i)]
            pay_df['is_use'] = pay_df['ds'].isin(ltv_days)
            result = pay_df[pay_df['is_use']]
            result['is_reg'] = result['account'].isin(reg_data.account.values)
            result = result[result['is_reg']]
            ltv_end_date = ds_add(date, i - 1)
            if ltv_end_date > final_date:
                data['d%d_pay_num' % i] = 0
                data['d%d_ltv' % i] = 0
            else:
                data['d%d_pay_num' % i] = result.account.nunique()
                data['d%d_ltv' %
                     i] = [result.sum().order_money * 1.0 / reg_num]

        result_df = DataFrame(data)
        columns = ['ds', 'reg_user_num'] + ['d%d_pay_num' % i for i in ltv_day
                                            ] + ['d%d_ltv' % i
                                                 for i in ltv_day]
        result_df = result_df[columns]
        dfs.append(result_df)
        # print result_df
    df = pd.concat(dfs)
    print df
    # 更新MySQL表
    table = 'dis_reg_user_ltv_bak'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    # update_mysql(table, df, del_sql, plat)
    # df.to_excel('/Users/kaiqigu/Downloads/Excel/vt_ltv.xlsx')
