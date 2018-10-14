#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 新增用户付费比例
'''
import settings_dev
from utils import hql_to_df, update_mysql, ds_add
import pandas as pd


# 付费占比日期
pay_days = [1, 2, 3, 7, 15, 30]


def dis_common_newpay_rate_ondate(date):
    reg_sql = "select ds,user_id from raw_registeruser where ds = '{0}'".format(
        date)
    pay_sql = '''
    select min(ds) as ds, user_id
    from raw_paylog
    where ds>='{date}' and ds<='{pay30_date}' and platform_2 != 'admin_test'
    group by user_id
    '''.format(**{
        'date': date,
        'pay30_date': ds_add(date, pay_days[5] - 1)
    })

    reg_df = hql_to_df(reg_sql)
    pay_df = hql_to_df(pay_sql)

    pay_dates_dic = {ds_add(date, pay_day - 1): 'd%d_pay_rate' % pay_day
                     for pay_day in pay_days}

    data_result = pay_df
    for i in enumerate(pay_days):
        if i[0] == 0:
            data_result = pay_df.loc[(pay_df['ds'] > ds_add(date, -1)) & (pay_df[
                'ds'] <= ds_add(date, i[1] - 1))]
        else:
            data = pay_df.loc[(pay_df['ds'] > ds_add(date, pay_days[i[0] - 1] - 1)) & (pay_df['ds'] <= ds_add(date, pay_days[i[0]] - 1))].copy()
            data['ds'] = ds_add(date, pay_days[i[0]] - 1)
            data_result = pd.concat([data_result, data])

    data_result['pay'] = 1
    pay_df_result = (data_result.pivot_table('pay', ['user_id'], 'ds')
                     .reset_index().merge(reg_df,
                                          on='user_id',
                                          how='right').reset_index())

    pay_df_result = pay_df_result.fillna(0)
    pay_df_finalresult = pay_df_result.groupby('ds').sum().reset_index()
    reg_num = reg_df.user_id.count()
    pay_df_finalresult['reg_num'] = reg_num
    pay_dates = [ds_add(date, pay_day - 1) for pay_day in pay_days]

    for i in pay_dates:
        pay_df_finalresult[i] = pay_df_finalresult[i] / reg_num
    pay_df_finalresult = pay_df_finalresult.rename(columns=pay_dates_dic)

    columns = ['ds', 'reg_num'] + ['d%d_pay_rate' % d for d in pay_days]
    result_df = pay_df_finalresult[columns]
    print result_df

    # 更新MySQL表
    table = 'dis_common_newpay_rate'
    del_sql = 'delete from {0} where ds = "{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)


def dis_common_newpay_rate(date):
    date_to_excute = ds_add(date, 1 - pay_days[-1])
    server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    if date_to_excute >= server_start_date:
        dis_common_newpay_rate_ondate(date_to_excute)

if __name__ == '__main__':
    settings_dev.set_env('metal_test')
    date = '20161019'
    dis_common_newpay_rate(date)
