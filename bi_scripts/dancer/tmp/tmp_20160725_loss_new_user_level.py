#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 武娘次日等级分布
'''
import settings_dev
from utils import hql_to_df, update_mysql, hqls_to_dfs, ds_add, date_range
import pandas as pd

settings_dev.set_env('dancer_ks')

def formatDate(Date, formatType='YYYY-MM-DD'):
    formatType = formatType.replace('YYYY', Date[0:4])
    formatType = formatType.replace('MM', Date[4:6])
    formatType = formatType.replace('DD', Date[-2:])
    return formatType

def lost_info(date):
    reg_time = formatDate(date)
    sql = '''
    select from_unixtime(cast(reg_time as int),'yyyy-MM-dd') as reg_date,level,count(distinct user_id) as num
    from parse_info
    where ds = '{date}'
      and from_unixtime(cast(reg_time as int),'yyyy-MM-dd') = '{reg_time}'
      and user_id not in (select user_id
                          from parse_info
                          where ds = '{tomorrow}'
                          )
    group by from_unixtime(cast(reg_time as int),'yyyy-MM-dd'),level
    '''.format(**{
        'date': date,
        'tomorrow': ds_add(date, 1),
        'reg_time': reg_time
    })
    print sql
    df = hql_to_df(sql)

    print df
    return df
# 执行
if __name__ == '__main__':
    result_dfs = []
    for days in date_range('20160801','20160803'):
        result_dfs.append(lost_info(days))
    final_result = pd.concat(result_dfs)
    final_result.to_excel('/Users/kaiqigu/Documents/dancer/tmp_20160804_new_user_level.xlsx')
