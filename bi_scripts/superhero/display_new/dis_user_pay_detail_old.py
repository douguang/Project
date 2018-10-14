#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 用户付费情况表
'''
from utils import hqls_to_dfs, update_mysql, date_range
import settings_dev
from pandas import DataFrame
from utils import nvl_data


def bi_sql(date, pp):
    info_sql = '''
    SELECT uid,
           account,
           regexp_replace(substr(create_time,1,10),'-','') reg_ds
    FROM raw_info
    WHERE ds = '{date}'
      AND substr(uid,1,1) = '{pp}'
    '''.format(date=date, pp=pp)
    account_sql = '''
    SELECT account,
           uid,
           regexp_replace(substr(create_time,1,10),'-','') reg_ds
    FROM mid_info_all
    WHERE ds ='{date}'
      AND substr(uid,1,1) = '{pp}'
    '''.format(date=date, pp=pp)
    pay_sql = '''
    SELECT ds,
           uid,
           regexp_replace(substr(order_time,1,10),'-','') order_ds,
           order_money order_rmb
    FROM mid_paylog_all
    WHERE ds = '{date}'
      AND platform_2 <> 'admin_test'
      AND substr(uid,1,1) = '{pp}'
    '''.format(date=date, pp=pp)
    return info_sql, account_sql, pay_sql


def foreign_sql(date):
    info_sql = '''
    SELECT uid,
           account,
           regexp_replace(substr(create_time,1,10),'-','') reg_ds
    FROM raw_info
    WHERE ds = '{0}'
    '''.format(date)
    account_sql = '''
    SELECT account,
           uid,
           regexp_replace(substr(create_time,1,10),'-','') reg_ds
    FROM mid_info_all
    WHERE ds ='{0}'
    '''.format(date)
    pay_sql = '''
    SELECT ds,
           uid,
           regexp_replace(substr(order_time,1,10),'-','') order_ds,
           order_money order_rmb
    FROM mid_paylog_all
    WHERE ds = '{0}'
      AND platform_2 <> 'admin_test'
    '''.format(date)
    return info_sql, account_sql, pay_sql


def dis_user_pay_detail_one(date, plat=None):
    plat = plat or settings_dev.platform
    print plat

    if plat == 'superhero_pub':
        info_sql, account_sql, pay_sql = bi_sql(date, 'g')
    elif plat == 'superhero_ios':
        info_sql, account_sql, pay_sql = bi_sql(date, 'a')
    elif plat == 'superhero_qiku':
        info_sql, account_sql, pay_sql = bi_sql(date, 'q')
    elif plat == 'superhero_tw':
        info_sql, account_sql, pay_sql = bi_sql(date, 't')
    else:
        info_sql, account_sql, pay_sql = foreign_sql(date)

    info_df, account_df, pay_df = hqls_to_dfs([info_sql, account_sql, pay_sql])

    pay_result_df = pay_df.merge(account_df, on='uid', how='left')

    # 新增account
    account_day_df = account_df.loc[account_df.reg_ds == date]
    account_ago_df = account_df.loc[account_df.reg_ds != date]
    account_day_df['is_reg'] = account_day_df['account'].isin(
        account_ago_df.account.values)
    new_account_df = account_day_df[~account_day_df['is_reg']]

    # 当日注册数据
    day_reg_user_df = info_df.loc[info_df.reg_ds == date]
    # 当日充值数据
    day_pay_df = pay_result_df.loc[pay_result_df.order_ds == date]
    # 以前充值数据
    ago_pay_df = pay_result_df.loc[pay_result_df.order_ds != date]

    # 新用户、dau、当日充值人数、DAU付费金额
    day_reg_user_df['is_ago'] = day_reg_user_df['account'].isin(
        new_account_df.account.values)
    day_reg_user_result_df = day_reg_user_df[day_reg_user_df['is_ago']]
    reg_user_num = day_reg_user_result_df.account.nunique()
    dau = info_df.account.nunique()
    pay_num = day_pay_df.account.nunique()
    income = day_pay_df.sum().order_rmb
    pay_rate = nvl_data(pay_num, dau)
    arppu = nvl_data(income, pay_num)
    arpu = nvl_data(income, dau)

    # 新增付费人数、金额
    day_pay_df['is_ago'] = day_pay_df['account'].isin(
        ago_pay_df.account.values)
    new_pay_df = day_pay_df[~day_pay_df['is_ago']]
    # new_pay_df['is_old'] = new_pay_df['account'].isin(new_account_df.account.values)
    # new_pay_df = new_pay_df[new_pay_df['is_old']]
    new_pay_num = new_pay_df.account.nunique()
    new_income = new_pay_df.sum().order_rmb
    new_pay_rate = nvl_data(new_pay_num, dau)
    new_arppu = nvl_data(new_income, new_pay_num)
    new_arpu = nvl_data(new_income, dau)

    # 当日新增付费人数、金额
    day_pay_df['is_reg'] = day_pay_df['account'].isin(
        day_reg_user_df.account.values)
    new_day_pay_df = day_pay_df[day_pay_df['is_reg']]
    new_day_pay_df['is_old'] = new_day_pay_df['account'].isin(
        new_account_df.account.values)
    new_day_pay_df = new_day_pay_df[new_day_pay_df['is_old']]
    new_day_pay_num = new_day_pay_df.account.nunique()
    new_day_income = new_day_pay_df.sum().order_rmb
    new_day_pay_rate = nvl_data(new_day_pay_num, reg_user_num)
    new_day_arppu = nvl_data(new_day_income, new_day_pay_num)
    new_day_arpu = nvl_data(new_day_income, reg_user_num)

    # 老玩家dau、付费人数、金额、付费率、arppu、arpu
    old_dau = dau - reg_user_num
    old_pay_num = pay_num - new_day_pay_num
    old_income = income - new_day_income
    old_pay_rate = nvl_data(old_pay_num, old_dau)
    old_arppu = nvl_data(old_income, old_pay_num)
    old_arpu = nvl_data(old_income, old_dau)

    result_df = DataFrame({
        'ds': [date],
        'reg_user_num': [reg_user_num],
        'dau': [dau],
        'pay_num': [pay_num],
        'income': [income],
        'pay_rate': [pay_rate],
        'arppu': [arppu],
        'arpu': [arpu],
        'new_pay_num': [new_pay_num],
        'new_income': [new_income],
        'new_pay_rate': [new_pay_rate],
        'new_arppu': [new_arppu],
        'new_arpu': [new_arpu],
        'new_day_pay_num': [new_day_pay_num],
        'new_day_income': [new_day_income],
        'new_day_pay_rate': [new_day_pay_rate],
        'new_day_arppu': [new_day_arppu],
        'new_day_arpu': [new_day_arpu],
        'old_dau': [old_dau],
        'old_pay_num': [old_pay_num],
        'old_income': [old_income],
        'old_pay_rate': [old_pay_rate],
        'old_arppu': [old_arppu],
        'old_arpu': [old_arpu]
    })

    column_list = ['pay_num', 'income', 'pay_rate', 'arppu', 'arpu']
    columns = ['ds', 'reg_user_num', 'dau'] + column_list + [
        'new_%s' % i for i in column_list
    ] + ['new_day_%s' % i for i in column_list
         ] + ['old_dau'] + ['old_%s' % i for i in column_list]
    result_df = result_df[columns]
    result_df = result_df.fillna(0)
    print result_df
    print '{0}.dis_user_pay_detail complete'.format(plat)

    # 更新MySQL表
    table = 'dis_user_pay_detail'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql, plat)


def dis_user_pay_detail(date):
    if settings_dev.code == 'superhero_bi':
        for plat in ['superhero_pub', 'superhero_ios']:
            print plat
            dis_user_pay_detail_one(date, plat)
    else:
        print settings_dev.code
        dis_user_pay_detail_one(date)


if __name__ == '__main__':
    # for platform in ['superhero_bi', 'superhero_qiku', 'superhero_vt',
    #                  'superhero_tl']:
    settings_dev.set_env('superhero_tw')
    # for date in date_range('20161014', '20161105'):
    dis_user_pay_detail('20170111')
    # settings_dev.set_env('superhero_bi')
    # date = '20161022'
    # dis_user_pay_detail(date)
