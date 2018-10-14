#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 新增uid用户等级分布详情
'''
import settings_dev
from utils import hql_to_df, update_mysql, ds_add, date_range
import pandas as pd

level_days = [8, 15, 31]


def dis_common_uid_level_detail_ondate(date):
    # 等级日期
    date_list = [ds_add(date, day - 1) for day in level_days]
    level_days_dic = {ds_add(date, level_day - 1): 'r%d_num' % level_day
                      for level_day in level_days}

    level_sql = '''
    SELECT ds,
           user_id,
           level
    FROM mid_info_all
    WHERE ds IN {date_list}
      AND user_id IN
        (SELECT user_id
         FROM raw_registeruser
         WHERE ds = '{date}')
    '''.format(date_list=tuple(date_list),
               date=date)
    level_df = hql_to_df(level_sql)

    level_list = range(0, 181, 5)

    result_df = level_df.groupby(
        ['ds', pd.cut(level_df.level, level_list)]).count().user_id.reset_index()
    result_df.level = result_df.level.astype('object')
    result_df = result_df.fillna(0)
    result_df = result_df.pivot_table(
        'user_id', ['level'], 'ds').reset_index().rename(columns=level_days_dic).fillna(0)
    result_df['ds'] = date

    level_days_col = ['r%d_num' % level_day for level_day in level_days]
    for col in level_days_col:
        if col not in result_df:
            result_df[col] = 0
    columns = ['ds', 'level'] + level_days_col
    result_df = result_df[columns]
    print result_df

    # 更新MySQL表
    table = 'dis_common_uid_level_detail'
    del_sql = 'delete from {0} where ds = "{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)


def dis_common_uid_level_detail(date):
    '''某天跑，更新会影响的日期数据
    '''
    server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    run_dates = filter(lambda d: d >= server_start_date, [ds_add(date, 1 - d)
                                                          for d in level_days])
    for f_date in run_dates:
        print f_date
        dis_common_uid_level_detail_ondate(f_date)


if __name__ == '__main__':
    for platform in ['sanguo_tw','sanguo_ks','sanguo_kr','sanguo_ios','sanguo_tl','sanguo_tx',]:
        settings_dev.set_env(platform)
        for date in date_range('20170307', '20170314'):
            dis_common_uid_level_detail(date)
    print "end"
