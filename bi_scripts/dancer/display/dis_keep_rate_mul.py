#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 用户 - 留存率
Time        : 2017.06.29
illustration: 未排除测试用户
'''
import settings_dev
import pandas as pd
from utils import ds_add
from utils import format_dates
from utils import hqls_to_dfs, hql_to_df
from utils import update_mysql
from utils import date_range
from dancer.cfg import LAN_TYPE

# 要跑的留存日期
keep_days = [2, 3, 4, 5, 6, 7, 14, 30, 60, 90]


def dis_keep_rate_mul(excute_date):
    '''在某一天执行'''
    server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    # 要抓取的注册日期
    reg_dates = filter(lambda d: d >= server_start_date,
                       [ds_add(excute_date, 1 - d) for d in keep_days])
    print reg_dates
    # 要抓取的活跃日期
    act_dates = set()
    for reg_date in reg_dates:
        act_dates.add(reg_date)
        for keep_day in keep_days:
            act_dates.add(ds_add(reg_date, keep_day - 1))

    reg_sql = '''
    SELECT regexp_replace(to_date(regist_time), '-', '') as ds,
           account,
           register_lan_sort as language
    FROM mid_info_all
    WHERE ds='{date}' and regexp_replace(to_date(regist_time), '-', '') IN {reg_dates} and regexp_replace(to_date(regist_time), '-', '')>='20170629'
    group by regexp_replace(to_date(regist_time), '-', ''),
           account,
           register_lan_sort
    '''.format(date=excute_date, reg_dates=format_dates(reg_dates))
    # print reg_sql
    act_sql = '''
    SELECT distinct ds,
           account,
           register_lan_sort as language
    FROM parse_info
    WHERE ds IN {act_dates}
    '''.format(act_dates=format_dates(act_dates))
    # act_df, reg_df = hqls_to_dfs([act_sql, reg_sql])
    act_df = hql_to_df(act_sql)
    reg_df = hql_to_df(reg_sql)

    # 活跃用户表转职后合并到注册表后，将活跃日期变成列名
    reg_df['reg'] = 1
    act_df['act'] = 1
    reg_act_df = (act_df.pivot_table('act', ['account', 'language'], 'ds')
                  .reset_index().merge(reg_df,
                                       on=['account', 'language'],
                                       how='right').reset_index())

    # dau
    dau_result = act_df.groupby(
        ['ds', 'language']).account.count().reset_index().rename(columns={
            'account': 'dau',
        })

    # 求每一个受影响的日期留存率，然后合并
    keep_rate_dfs = []
    for reg_date in reg_dates:
        act_dates = [ds_add(reg_date, keep_day - 1) for keep_day in keep_days]
        act_dates_dic = {ds_add(reg_date, keep_day - 1): 'd%d_keep' % keep_day
                         for keep_day in keep_days}
        keep_df = (reg_act_df.loc[reg_act_df.ds == reg_date, [
            'reg', 'language'
        ] + act_dates_dic.keys()].rename(columns=act_dates_dic).groupby(
            'language').sum().reset_index().fillna(0))
        result = dau_result.loc[dau_result.ds == reg_date].merge(
            keep_df, on='language', how='outer').fillna(0)
        for c in act_dates_dic.values():
            result[c + 'rate'] = result[c] / result.reg
        result = result.fillna(0)
        keep_rate_dfs.append(result)

    keep_rate_df = pd.concat(keep_rate_dfs)
    columns = ['ds', 'reg', 'dau', 'language'] + ['d%d_keeprate' % d
                                                  for d in keep_days]
    result_df = keep_rate_df[columns]
    rename_dic = {'reg': 'reg_user_num'}
    result_df = result_df.rename(columns=rename_dic)
    result_df = result_df.replace({'language': LAN_TYPE})
    print result_df

    # 更新MySQL
    table = 'dis_keep_rate'
    del_sql = 'delete from {0} where ds in {1}'.format(table,
                                                       format_dates(reg_dates))
    column = ['ds', 'reg_user_num', 'dau', 'language'] + ['d%d_keeprate' % d
                                                          for d in keep_days]
    update_mysql(table, result_df[column], del_sql)

    print '{0} complete'.format(table)


if __name__ == '__main__':
    settings_dev.set_env('dancer_mul')
    for date in date_range('20171026', '20171109'):
        dis_keep_rate_mul(date)
