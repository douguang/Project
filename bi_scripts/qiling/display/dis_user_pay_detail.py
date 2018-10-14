#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : kailiang li
Description : 用户付费情况表
'''
from utils import hqls_to_dfs, update_mysql, date_range
import settings_dev
from pandas import DataFrame

def dis_user_pay_detail(date):
    table = 'dis_user_pay_detail'
    print table
    info_sql = '''
    SELECT user_id,
           account,
           regexp_replace(substr(reg_time,1,10),'-','') reg_ds
    FROM raw_info
    WHERE ds = '{0}'
    '''.format(date)
    account_sql = '''
    SELECT account,
           user_id
    FROM mid_info_all
    WHERE ds = '{0}'
    '''.format(date)
    pay_sql = '''
    SELECT ds,
           user_id,
           order_money
    FROM raw_paylog
    WHERE ds <= '{0}'
      AND platform_2 <> 'admin_test'
      AND order_id not like '%testktwwn%'
    '''.format(date)
    info_df, account_df, pay_df = hqls_to_dfs([info_sql, account_sql, pay_sql])

    pay_result_df = pay_df.merge(account_df,on = 'user_id', how = 'left')

    # account 用户数据
    account_df['num'] = 1
    account_data = account_df.groupby('account').sum().num.reset_index()
    # 滚服的account
    gun_account_df = account_data[account_data.num > 1]

    # 当日注册数据
    day_reg_user_df = info_df.loc[info_df.reg_ds == date]
    # 当日充值数据
    day_pay_df = pay_result_df.loc[pay_result_df.ds == date]
    # 以前充值数据
    ago_pay_df = pay_result_df.loc[pay_result_df.ds != date]

    # 新用户、dau、当日充值人数、DAU付费金额
    reg_user_num = day_reg_user_df.account.nunique()
    dau = info_df.account.nunique()
    pay_num = day_pay_df.account.nunique()
    income = day_pay_df.sum().order_money
    pay_rate = pay_num * 1.0 / dau
    arppu = income * 1.0 / pay_num
    arpu = income * 1.0 / dau

    # 新增付费人数、金额
    day_pay_df['is_ago'] = day_pay_df['account'].isin(ago_pay_df.account.values)        #新增一列'is_ago'，用来记录今天付费用户之前是否付费，若付费则取1否在取0
    new_pay_df = day_pay_df[~day_pay_df['is_ago']]                                      #将'is_ago'取反，用来记录是否为新付费用户
    new_pay_df['is_old'] = new_pay_df['account'].isin(gun_account_df.account.values)
    new_pay_df = new_pay_df[~new_pay_df['is_old']]
    new_pay_num = new_pay_df.account.nunique()
    new_income = new_pay_df.sum().order_money
    new_pay_rate = new_pay_num * 1.0 / dau
    new_arppu = new_income * 1.0 / new_pay_num
    new_arpu = new_income * 1.0 / dau

    # 当日新增付费人数、金额
    day_pay_df['is_reg'] = day_pay_df['account'].isin(day_reg_user_df.account.values)   #判断付费用户是否为当日注册
    new_day_pay_df = day_pay_df[day_pay_df['is_reg']]
    new_day_pay_df['is_old'] = new_day_pay_df['account'].isin(gun_account_df.account.values)
    new_day_pay_df = new_day_pay_df[~new_day_pay_df['is_old']]
    new_day_pay_num = new_day_pay_df.account.nunique()
    new_day_income = new_day_pay_df.sum().order_money
    new_day_pay_rate = new_day_pay_num * 1.0 / reg_user_num
    new_day_arppu = new_day_income * 1.0 / new_day_pay_num
    new_day_arpu = new_day_income * 1.0 / reg_user_num

    # 老玩家dau、付费人数、金额、付费率、arppu、arpu
    old_dau = dau - reg_user_num
    old_pay_num = pay_num - new_day_pay_num
    old_income = income - new_day_income
    old_pay_rate = 0
    old_arpu = 0
    old_arppu = 0
    if old_dau != 0:
        old_pay_rate = old_pay_num * 1.0 / old_dau
        old_arpu = old_income * 1.0 / old_dau
    if old_pay_num !=0:
        old_arppu = old_income * 1.0 / old_pay_num

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
    columns = ['ds', 'reg_user_num','dau'] + column_list + [
        'new_%s' % i for i in column_list
    ] + ['new_day_%s' % i for i in column_list
         ] + ['old_dau'] + ['old_%s' % i for i in column_list]
    result_df = result_df[columns]
    result_df = result_df.fillna(0)
    #print result_df
    print 'dis_user_pay_detail complete'
    #result_df.to_excel("/home/kaiqigu/桌面/用户付费情况_修改_%s.xlsx" % date,index=False)
    # 更新MySQL表

    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)

    return result_df

if __name__ == '__main__':
    for platform in ['sanguo_in','sanguo_tx','sanguo_kr']:
          settings_dev.set_env(platform)
          for date in date_range('20161208','20161211'):
                result = dis_user_pay_detail(date)
    #           result.to_excel('/home/kaiqigu/Documents/dancer.xlsx')
    print "end"


