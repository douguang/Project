#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Desctiption : 钻石消耗与获取
Time        : 2017-03-16
'''
import settings_dev
from utils import hql_to_df

if __name__ == '__main__':
    settings_dev.set_env('jianniang_test')
    date = '20170316'
    actionlog_sql = '''
    SELECT user_id,
           coin_diff,
           a_typ
    FROM parse_actionlog
    WHERE ds ='{date}'
    and log_t >='2017-03-16 00:00:00'
    and log_t <='2017-03-16 23:59:59'
    and  user_id <> 'None'
    and coin_diff is not null
    '''.format(date=date)
    actionlog_df = hql_to_df(actionlog_sql)
    # actionlog_df['spend'] = actionlog_df['coin_diff'].map(
    #     lambda s: s if s < 0 else 0)
    result_df = actionlog_df.groupby('a_typ').sum().reset_index()


    result_df.to_excel('/Users/kaiqigu/Documents/h5_data/coin_df.xlsx')
