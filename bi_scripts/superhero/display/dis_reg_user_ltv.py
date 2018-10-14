#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 营收 - 注册用户LTV
Time        : 2017.05.08
illustration:
'''
import settings_dev
import pandas as pd
from pandas import DataFrame
from utils import ds_add
from utils import hqls_to_dfs
from utils import update_mysql,date_range
from sqls_for_games.superhero import gs_sql

# Ltv日期
ltv_days = [1, 2, 3, 4, 5, 6, 7, 14, 30, 60, 90, 120, 150, 180]


def dis_sup_act_ltv_ondate(date, exec_date):
    ltv_date_list = [ds_add(date, day - 1) for day in ltv_days]
    ltv_num_dic = {ds_add(date, day - 1) + '_num': 'd%d_pay_num' % day
                   for day in ltv_days}
    ltv_money_dic = {ds_add(date, day - 1) + '_money': 'd%d_money_ltv' % day
                     for day in ltv_days}
    ltv_rate_dic = {ds_add(date, day - 1) + '_rate': 'd%d_ltv' % day
                    for day in ltv_days}
    new_act_sql = '''
    select t2.reg_ds as ds,t1.account,substring(t1.uid,1,1) as plat,t1.uid from (
          select account,uid from mid_info_all where ds='{date}' and account != '' group by account,uid
        )t1 left outer join(
          select account,min(regexp_replace(to_date(create_time),'-','')) as reg_ds from mid_info_all where ds='{date}' and account != '' group by account
        )t2 on t1.account=t2.account
        where t2.reg_ds = '{date}'
        group by ds,t1.account,plat,t1.uid
    '''.format(date=date)
    pay_sql = '''
    SELECT ds,
           uid,
           substr(uid,1,1) as plat,
           order_money as order_rmb
    FROM raw_paylog
    WHERE ds>='{date}'
      AND ds<='{max_date}'
    '''.format(date=date, max_date=ds_add(date, ltv_days[-1] - 1))

    new_act_df, pay_df, gs_df = hqls_to_dfs([new_act_sql, pay_sql, gs_sql])
    # 排除开服至今的gs数据
    gs_df = gs_df.rename(columns={'user_id': 'uid'})
    new_act_df = new_act_df[~new_act_df['uid'].isin(gs_df.uid.values)]
    pay_df = pay_df[~pay_df['uid'].isin(gs_df.uid.values)]
    df_list = []
    for plat in new_act_df.drop_duplicates('plat').plat.tolist():
        plat_act_df = new_act_df.loc[new_act_df.plat == plat]
        plat_pay_df = pay_df.loc[pay_df.plat == plat]
        new_act_num = plat_act_df.drop_duplicates(['account']).count().account
        # pay_df = pay_df.rename(columns={'uid': 'uid'})
        result_data = {}
        for i in ltv_date_list:
            data = plat_pay_df.loc[(plat_pay_df['ds'] >= date) & (plat_pay_df[
                'ds'] <= i)]
            result = data.merge(new_act_df, on='uid')
            print i, exec_date
            if i > exec_date:
                pay_num_rate = 0
                pay_num = 0
                pay_money = 0
            else:
                pay_num = result.uid.nunique()
                pay_money = result.sum().order_rmb
                pay_num_rate = pay_money * 1.0 / new_act_num
            result_data[i + '_rate'] = [pay_num_rate]
            result_data[i + '_num'] = [pay_num]
            # result_data[i + '_money'] = [pay_money]
        result_data['reg_num'] = [new_act_num]
        result_data = DataFrame(result_data)
        result_data = result_data.rename(columns=ltv_num_dic).rename(
            columns=ltv_money_dic).rename(columns=ltv_rate_dic)
        # del result_data['d1_pay_num']
        # del result_data['d1_ltv']
        result_data['ds'] = date
        rename_dic = {'reg_num': 'reg_user_num'}
        result_data = result_data.rename(columns=rename_dic)
        result_data['plat'] = plat
        print result_data
        df_list.append(result_data)

    result_df = pd.concat(df_list)

    # 更新MySQL表
    table = 'dis_reg_user_ltv_new'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    column = ['ds', 'reg_user_num'] + \
        ['d%d_pay_num' % day for day in ltv_days[:]] + \
        ['d%d_ltv' % day for day in ltv_days[:]]
    if settings_dev.code == 'superhero_bi':
        pub_result_df = result_df[result_df.plat == 'g'][column]
        ios_result_df = result_df[result_df.plat == 'a'][column]
        update_mysql(table, pub_result_df, del_sql, 'superhero_pub')
        update_mysql(table, ios_result_df, del_sql, 'superhero_ios')
    else:
        update_mysql(table, result_df[column], del_sql)

    print '{0} is complete'.format(table)


def dis_reg_user_ltv(date):
    exec_date = date
    server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    # 要抓取的注册日期
    reg_dates = filter(lambda d: d >= server_start_date,
                       [ds_add(date, 1 - d) for d in ltv_days])
    for day in reg_dates:
        print day
        dis_sup_act_ltv_ondate(day, exec_date)


if __name__ == '__main__':
    settings_dev.set_env('superhero_bi')
    for date in date_range('20171116','20171127'):
        print '======'
        print date
        dis_reg_user_ltv(date)