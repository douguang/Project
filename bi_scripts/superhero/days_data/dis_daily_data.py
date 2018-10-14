#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 日常数据(批量补数据)
注：需手动删除更新的数据
'''
from utils import hqls_to_dfs, ds_add, update_mysql, hql_to_df, date_range
import settings
import pandas as pd

if __name__ == '__main__':
    settings.set_env('superhero_bi')
    plat = 'superhero_pub'
    start_date = '20160501'
    end_date = '20160824'
    final_date = '20160824'

    date_list = date_range(start_date, end_date)
    reg_dates = [date for date in date_list]

    act_sql = '''
    SELECT ds,
           uid,
           platform_2
    FROM raw_info
    WHERE substr(uid,1,1) = 'g'
and ds >= '{0}'
    '''.format(ds_add(start_date,-30))
    reg_sql = '''
    SELECT  ds,
            uid
       FROM raw_reg
       WHERE substr(uid,1,1) = 'g'
and ds >= '{0}'
       and ds<= '{1}'
    '''.format(start_date, end_date)
    pay_ago_sql = '''
    SELECT uid,
           regexp_replace(substr(min(order_time),1,10),'-','') pay_time
    FROM mid_paylog_all
    WHERE substr(uid,1,1) = 'g'
and platform_2 <> 'admin_test'
    GROUP BY uid
    '''.format(ds_add(date, -1))
    pay_sql = '''
    SELECT ds,
           uid,
           order_rmb
    FROM raw_paylog
    WHERE substr(uid,1,1) = 'g'
and platform_2 <> 'admin_test'
      AND ds >= '{0}'
       and ds<= '{1}'
    '''.format(start_date, end_date)
    bi_pay_sql = '''
    SELECT ds,
           uid,
           order_money order_rmb
    FROM raw_paylog
    WHERE substr(uid,1,1) = 'g'
and platform_2 <> 'admin_test'
      AND ds >= '{0}'
       and ds<= '{1}'
    '''.format(start_date, end_date)
    act_df, reg_df, pay_ago_df, pay_df = hqls_to_dfs(
        [act_sql, reg_sql, pay_ago_sql, bi_pay_sql])
    # if plat in ['superhero_pub','superhero_ios','superhero_qiku']:
    #     pay_df = hql_to_df(bi_pay_sql)
    # else:
    #     pay_df = hql_to_df(pay_sql)
    reg_df = reg_df.merge(act_df, on=['ds', 'uid'], how='left')
    pay_df = pay_df.merge(act_df, on=['ds', 'uid'], how='left')

    # 收入
    pay_money_df = pay_df.groupby(['ds', 'platform_2']).sum().reset_index(
    ).loc[:, ['ds', 'platform_2', 'order_rmb']].rename(
        columns={'order_rmb': 'pay_money'})
    # 充值人数
    pay_num_df = (pay_df.drop_duplicates(['ds','uid'])
                  .groupby(['ds', 'platform_2']).count().reset_index()
                  .loc[:, ['ds', 'platform_2', 'uid']]
                  .rename(columns={'uid': 'pay_num'}))
    # 新增充值人数
    pay_ago_df = pay_ago_df.rename(columns={'pay_time': 'ds'})
    pay_ago_df = pay_ago_df.merge(act_df, on=['ds', 'uid'], how='left')

    pay_new_df = (pay_ago_df.groupby(['ds', 'platform_2'])
                  .count().reset_index()
                  .rename(columns={'uid': 'pay_new_num'}))
    # 新用户
    reg_num_df = (reg_df.groupby(['ds', 'platform_2']).count().reset_index()
                  .loc[:, ['ds', 'platform_2', 'uid']]
                  .rename(columns={'uid': 'reg_num'}))
    # 活跃用户数

    dfs = []
    for date in reg_dates:
        print date
        act_days = [1, 7, 30]
        act_dates = [ds_add(date, 1 - act_day) for act_day in act_days]
        act_dates_dic = {ds_add(date, 1 - act_day): 'd%d_num' % act_day
                         for act_day in act_days}
        act_list = []
        for i in act_dates:
            act_result = act_df.loc[(act_df.ds >= i) & (act_df.ds <= date)]
            act_result.loc[:, ['ds']] = i
            act_result = act_result.drop_duplicates(['uid'])
            act_list.append(act_result)
        act_num_df = pd.concat(act_list)
        act_num_df['act'] = 1
        act_finnum_df = (
            act_num_df.pivot_table('act', ['uid', 'platform_2'], 'ds')
            .reset_index().groupby('platform_2').sum().reset_index()
            .rename(columns=act_dates_dic))
        act_finnum_df['ds'] = date
        dfs.append(act_finnum_df)
    act_result_df = pd.concat(dfs)

    result_df = (reg_num_df
                .merge(pay_num_df,on=['ds','platform_2'],how='outer')
                .merge(pay_new_df,on=['ds','platform_2'],how='outer')
                .merge(pay_money_df, on=['ds','platform_2'],how='outer')
                .merge(act_result_df,on=['ds','platform_2'],how='outer')
                .fillna(0))
    # 付费率
    result_df['pay_rate'] = result_df['pay_num'] * 1.0 / result_df['d1_num']
    # arpu
    result_df['arpu'] = result_df['pay_money'] * 1.0 / result_df['d1_num']
    # arppu
    result_df['arppu'] = result_df['pay_money'] * 1.0 / result_df['pay_num']

    result_df = result_df.fillna(0)

    columns = ['ds', 'platform_2', 'reg_num', 'd1_num', 'd7_num', 'd30_num',
               'pay_num', 'pay_new_num', 'pay_money', 'pay_rate', 'arpu',
               'arppu']
    result_df = result_df[columns]
    rename_dic = {'platform_2':'platform','reg_num':'reg_user_num','d1_num':'dau',
                'd7_num':'wau','d30_num':'mau','pay_new_num':'new_pay_num',
                'pay_money':'income','pay_rate':'spend_rate'}
    result_df = result_df.rename(columns=rename_dic)
    result_df = result_df.loc[(result_df.ds >=start_date) & (result_df.ds <=end_date)]
    print result_df

    # 更新MySQL表
    table = 'dis_daily_data'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, tuple(reg_dates))
    update_mysql(table, result_df, del_sql, plat)
