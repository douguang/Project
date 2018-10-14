#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 注册用户LTV
'''
from utils import hql_to_df, ds_add, ds_delta, format_dates, date_range, update_mysql
import pandas as pd
import settings_dev


def load_actionlog(date):
    settings_dev.set_env('dancer_pub')
    sql = '''
    SELECT user_id,
           a_typ,
           log_t
    FROM
      (SELECT user_id,
              a_typ,
              log_t,
              row_number() over(partition BY user_id
                                ORDER BY log_t DESC) AS rn
       FROM parse_actionlog
       WHERE ds = '{date}'
         AND a_typ NOT LIKE '%index%'
         AND a_typ NOT IN ('user.refresh', 'user.npc_info', 'user.main_page', 'user.get_latest_loudspeaker', 'user.get_latest_chat'))a2
    WHERE a2.rn <= 10
    '''.format(date=date)
    df = hql_to_df(sql)
    print df
    df.to_csv(r'C:\workflow\Temporary-demand\dancer\2016-12-01\user_10act_%s.txt' % date, index=False)

if __name__ == '__main__':
    for date in date_range('20161115', '20161129'):
        load_actionlog(date)
