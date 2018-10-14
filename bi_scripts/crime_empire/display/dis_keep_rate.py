#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 留存率
'''
from utils import hql_to_df, hqls_to_dfs, ds_add, update_mysql, date_range
import settings_dev
import pandas as pd
from pandas import DataFrame

keep_days = [1, 2, 3, 4, 5, 6, 7, 14, 30, 60, 90]
def dis_keep_rate(date):
    server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    date_list = filter(lambda d: d >= server_start_date, [ds_add(date, 1 - d) for d in keep_days])
    df = DataFrame()
    else_list = filter(lambda d: d >= server_start_date, [ds_add(date, d - 1) for d in keep_days])
    df['ds'] = else_list
    df['reg_date'] = date
    keep_dfs = []
    for dates in tuple(date_list):
        on_list = filter(lambda d: d >= server_start_date, [ds_add(dates, d - 1) for d in keep_days])
        rate_sql = '''
        select ds, '{dates}' as reg_date, count(distinct account) as user_num
        from parse_info
        where ds in {on_list}
        and account not in(
          select account
          from mid_info_all
          where ds = '{dates_ago}'
          )
        and account in(
          select account
          from parse_info
          where ds = '{dates}'
          )
        group by ds
        order by ds
        '''.format(dates=dates, dates_ago=ds_add(dates, -1), on_list=tuple(on_list))
        keep_df = hql_to_df(rate_sql)
        keep_dfs.append(keep_df)
    final_df = pd.concat(keep_dfs)
    dau_sql = '''
    select ds,count(distinct account) as dau
    from parse_info
    where ds in {dates}
    group by ds
    order by ds
    '''.format(dates=tuple(date_list))
    dau_df = hql_to_df(dau_sql)
    # print dau_df
    ori_df = df.merge(final_df, on=['ds', 'reg_date'], how='outer').fillna(0)
    ori_df['days'] = (pd.to_datetime(ori_df['ds']) - pd.to_datetime(ori_df['reg_date'])).dt.days + 1
    mid_df = pd.pivot_table(ori_df, index=['reg_date'], columns=['days'], fill_value=0).reset_index()
    mid_df.columns = ['ds'] + ['d%d_user' % d for d in keep_days]
    print mid_df
    for d in mid_df.d1_user:
        print d
    dau_df['reg_user_num'] = mid_df['d1_user']
    for i in keep_days:
        d = str(i)
        dau_df['d' + d + '_keeprate'] = mid_df['d' + d + '_user'] / dau_df['reg_user_num']
    dau_df = dau_df.drop('d1_keeprate',axis=1)
    result_df = dau_df.fillna(0)
    # 更新MySQL表
    table = 'dis_keep_rate'
    print date, table
    del_sql = 'delete from {0} where ds in {1}'.format(table, tuple(date_list))
    update_mysql(table, result_df, del_sql)

if __name__ == '__main__':
    # settings_dev.set_env('dancer_tw')
    # dis_keep_rate('20160802')
    for platform in ('crime_empire_pub',):
        settings_dev.set_env(platform)
        for date in date_range('20180129', '20180130'):
            print date
            dis_keep_rate(date)
