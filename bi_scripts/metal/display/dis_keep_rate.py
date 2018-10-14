#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 用户留存率
'''
from utils import hql_to_df, ds_add, update_mysql, format_dates, date_range
import settings_dev
import pandas as pd

# 要跑的留存日期
keep_days = [2, 3, 4, 5, 6, 7, 14, 30, 60, 90]

def dis_keep_rate(excute_date):
    '''在某一天执行'''
    server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    # 要抓取的注册日期
    reg_dates = filter(lambda d: d >= server_start_date, [ds_add(excute_date, 1 - d) for d in keep_days])
    if not reg_dates:
        return
    # 要抓取的活跃日期
    act_dates = set()
    for reg_date in reg_dates:
        act_dates.add(reg_date)
        for keep_day in keep_days:
            act_dates.add(ds_add(reg_date, keep_day - 1))

    reg_sql = '''
    select regexp_replace(to_date(reg_time),'-','') as reg_ds,user_id from raw_info where ds in {reg_dates} and regexp_replace(to_date(reg_time),'-','')  in {reg_dates}
    group by reg_ds,user_id
    '''.format(reg_dates=format_dates(reg_dates))
    reg_df = hql_to_df(reg_sql)
    reg_df['reg_user_num'] = 1

    act_sql = '''
    select ds as act_ds, user_id
    from raw_info
    where ds in {act_dates}
    '''.format(act_dates=format_dates(act_dates))
    act_df = hql_to_df(act_sql)

    # 活跃用户表转职后合并到注册表后，将活跃日期变成列名
    act_df['act'] = 1
    reg_act_df = (act_df
                  .pivot_table('act', ['user_id'], 'act_ds')
                  .reset_index()
                  .merge(reg_df, how='right')
                  .reset_index()
                  )

    # 求每一个受影响的日期留存率，然后合并
    keep_rate_dfs = []
    for reg_date in reg_dates:
        act_dates = [ds_add(reg_date, keep_day - 1) for keep_day in keep_days]
        act_dates_dic = {ds_add(reg_date, keep_day - 1): 'd%d_keep' % keep_day for keep_day in keep_days}
        keep_df = (reg_act_df
                   .loc[reg_act_df.reg_ds == reg_date,
                        ['reg_user_num'] + act_dates_dic.keys()]
                   .rename(columns=act_dates_dic)
                   .sum()
                   .fillna(0)
                   .to_frame()
                   .T
                   )
        for c in act_dates_dic.values():
            keep_df[c + 'rate'] = keep_df[c] / keep_df.reg_user_num
        keep_df['ds'] = reg_date
        keep_df['dau'] = len(set(act_df.loc[act_df.act_ds == reg_date].user_id.tolist()) | set(reg_df.loc[reg_df.reg_ds == reg_date].user_id.tolist()))
        keep_rate_dfs.append(keep_df)

    keep_rate_df = pd.concat(keep_rate_dfs)
    columns = ['ds', 'dau', 'reg_user_num'] + ['d%d_keeprate' % d for d in keep_days]
    result_df = keep_rate_df[columns]

    # 更新MySQL表
    table = 'dis_keep_rate'
    del_sql = 'delete from {0} where ds in {1}'.format(table, format_dates(reg_dates))
    update_mysql(table, result_df, del_sql)

def keep_rate_date(date):
    '''跑某一天的留存'''
    act_dates = [ds_add(date, keep_day - 1) for keep_day in keep_days] + [date]
    act_dates_dic = {ds_add(date, keep_day - 1): 'd%d_keep' % keep_day for keep_day in keep_days}

    reg_sql = '''
    select regexp_replace(to_date(reg_time),'-','') as reg_ds,user_id from raw_info where ds in {date} and regexp_replace(to_date(reg_time),'-','')  in {date}
    group by reg_ds,user_id
    '''.format(date=date)
    reg_df = hql_to_df(reg_sql)
    reg_df['reg_user_num'] = 1

    act_sql = '''
    select ds as act_ds, user_id
    from raw_info
    where ds in {act_dates}
    '''.format(act_dates=format_dates(act_dates))
    act_df = hql_to_df(act_sql)

    dau_df = act_df.groupby('act_ds').count().reset_index()

    act_df['act'] = 1
    reg_act_df = (act_df
                  .pivot_table('act', ['user_id'], 'act_ds')
                  .reset_index()
                  .merge(reg_df, how='right')
                  .reset_index()
                  )

    reg_date = date

    keep_df = reg_act_df.loc[reg_act_df.reg_ds == reg_date, ['reg_user_num'] + act_dates_dic.keys()].rename(columns=act_dates_dic).sum().fillna(0).to_frame().T
    for c in act_dates_dic.values():
        keep_df[c + 'rate'] = keep_df[c] / keep_df.reg_user_num
    keep_df['ds'] = reg_date
    keep_df['dau'] = dau_df.loc[dau_df.act_ds == reg_date, 'user_id'].values[0]
    columns = ['ds', 'dau', 'reg_user_num'] + ['d%d_keeprate' % d for d in keep_days]
    daily_keep_rate_df = keep_df[columns]
    return daily_keep_rate_df

if __name__ == '__main__':
    for platform in ['metal_pub',]:
        settings_dev.set_env(platform)
        for date in date_range('20180314', '20180317'):
            dis_keep_rate(date)
