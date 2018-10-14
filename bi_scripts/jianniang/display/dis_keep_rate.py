#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Desctiption : 留存率
Time        : 2017.03.16
'''
import settings_dev
import pandas as pd
from utils import hql_to_df
from utils import ds_add
from utils import update_mysql
from utils import format_dates
# from utils import date_range


# 要跑的留存日期
keep_days = [2, 3, 4, 5, 6, 7, 14, 30, 60, 90]


def dis_keep_rate(excute_date):
    '''在某一天执行'''
    server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    # 要抓取的注册日期
    reg_dates = filter(lambda d: d >= server_start_date,
                       [ds_add(excute_date, 1 - d) for d in keep_days])
    if not reg_dates:
        return
    # 要抓取的活跃日期
    act_dates = set()
    for reg_date in reg_dates:
        act_dates.add(reg_date)
        for keep_day in keep_days:
            act_dates.add(ds_add(reg_date, keep_day - 1))


    # user_id 留存
    user_id_reg_sql = '''
    SELECT distinct regexp_replace(substr(reg_time,1,10),'-','') AS reg_ds ,
           user_id
    FROM raw_info
    WHERE ds IN {reg_dates}
    AND regexp_replace(substr(reg_time,1,10),'-','') IN {reg_dates}
    '''.format(reg_dates=format_dates(reg_dates))
    user_id_reg_df = hql_to_df(user_id_reg_sql)
    user_id_reg_df['reg_user_num'] = 1

    user_id_act_sql = '''
    select ds as act_ds, user_id
    from raw_info
    where ds in {act_dates}
    '''.format(act_dates=format_dates(act_dates))
    user_id_act_df = hql_to_df(user_id_act_sql)

    # 活跃用户表转职后合并到注册表后，将活跃日期变成列名
    user_id_act_df['act'] = 1
    user_id_reg_act_df = (user_id_act_df.pivot_table('act', ['user_id'], 'act_ds')
                  .reset_index().merge(user_id_reg_df, how='right').reset_index())

    # 求每一个受影响的日期留存率，然后合并
    keep_rate_dfs = []
    for reg_date in reg_dates:
        # act_dates = [ds_add(reg_date, keep_day - 1) for keep_day in keep_days]
        act_dates_dic = {ds_add(reg_date, keep_day - 1): 'd%d_keep' % keep_day
                         for keep_day in keep_days}
        user_id_keep_df = (user_id_reg_act_df.loc[user_id_reg_act_df.reg_ds == reg_date,
                                  ['reg_user_num'] + act_dates_dic.keys()]
                   .rename(columns=act_dates_dic).sum().fillna(0).to_frame().T)
        for c in act_dates_dic.values():
            user_id_keep_df[c + 'rate'] = user_id_keep_df[c] / user_id_keep_df.reg_user_num
        user_id_keep_df['ds'] = reg_date
        user_id_keep_df['dau'] = len(set(user_id_act_df.loc[
            user_id_act_df.act_ds == reg_date].user_id.tolist()) | set(user_id_reg_df.loc[
                user_id_reg_df.reg_ds == reg_date].user_id.tolist()))
        keep_rate_dfs.append(user_id_keep_df)

    keep_rate_df = pd.concat(keep_rate_dfs)
    columns = ['ds', 'dau', 'reg_user_num'] + ['d%d_keeprate' % d
                                               for d in keep_days]
    result_df = keep_rate_df[columns]
    print result_df

    # 更新MySQL表
    table = 'dis_keep_rate_user_id'
    del_sql = 'delete from {0} where ds in {1}'.format(table,
                                                       format_dates(reg_dates))
    update_mysql(table, result_df, del_sql)


    # account 留存
    account_reg_sql = '''
    SELECT min(regexp_replace(substr(reg_time,1,10),'-','')) AS reg_ds,
           account
    FROM raw_info
    WHERE ds IN {reg_dates}
    AND regexp_replace(substr(reg_time,1,10),'-','') IN {reg_dates} group by account
    '''.format(reg_dates=format_dates(reg_dates))
    account_reg_df = hql_to_df(account_reg_sql)
    account_reg_df['reg_user_num'] = 1

    account_act_sql = '''
    select ds as act_ds, account
    from raw_info
    where ds in {act_dates} group by account, ds
    '''.format(act_dates=format_dates(act_dates))
    account_act_df = hql_to_df(account_act_sql)

    # 活跃用户表转职后合并到注册表后，将活跃日期变成列名
    account_act_df['act'] = 1
    account_reg_act_df = (account_act_df.pivot_table('act', ['account'], 'act_ds')
                  .reset_index().merge(account_reg_df, how='right').reset_index())

    # 求每一个受影响的日期留存率，然后合并
    keep_rate_dfs = []
    for reg_date in reg_dates:
        # act_dates = [ds_add(reg_date, keep_day - 1) for keep_day in keep_days]
        act_dates_dic = {ds_add(reg_date, keep_day - 1): 'd%d_keep' % keep_day
                         for keep_day in keep_days}
        account_keep_df = (account_reg_act_df.loc[account_reg_act_df.reg_ds == reg_date,
                                  ['reg_user_num'] + act_dates_dic.keys()]
                   .rename(columns=act_dates_dic).sum().fillna(0).to_frame().T)
        for c in act_dates_dic.values():
            account_keep_df[c + 'rate'] = account_keep_df[c] / account_keep_df.reg_user_num
        account_keep_df['ds'] = reg_date
        account_keep_df['dau'] = len(set(account_act_df.loc[
            account_act_df.act_ds == reg_date].account.tolist()) | set(account_reg_df.loc[
                account_reg_df.reg_ds == reg_date].account.tolist()))
        keep_rate_dfs.append(account_keep_df)

    keep_rate_df = pd.concat(keep_rate_dfs)
    columns = ['ds', 'dau', 'reg_user_num'] + ['d%d_keeprate' % d
                                               for d in keep_days]
    result_df = keep_rate_df[columns]
    print result_df
    # 更新MySQL表
    table = 'dis_keep_rate_account'
    del_sql = 'delete from {0} where ds in {1}'.format(table,
                                                       format_dates(reg_dates))
    update_mysql(table, result_df, del_sql)


if __name__ == '__main__':
    settings_dev.set_env('jianniang_tw')
    date = '20170709'
    dis_keep_rate(date)
