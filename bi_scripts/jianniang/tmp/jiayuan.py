#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 家园、行侠、国战
Time        : 2017.03.17
'''
import settings_dev
from utils import hqls_to_dfs

settings_dev.set_env('jianniang_test')
jiayuan_sql = '''
SELECT user_id,
       a_typ
FROM parse_actionlog
WHERE log_t >='2017-03-19 00:00:00'
  AND log_t <='2017-03-19 23:59:59'
  AND user_id <> 'None'
  AND a_typ IN ( 'friend_visit_req' ,
                 'friend_speedup_main_req' ,
                 'friend_speedup_req' ,
                 'friend_steal_req' ,
                 'chivalry_main_req' ,
                 'chivalry_refresh_req' ,
                 'chivalry_start_req' ,
                 'chivalry_done_req' ,
                 'chivalry_cancel_req' )
'''
mafiabattle_sql = '''
SELECT user_id,
       a_typ
FROM parse_actionlog
WHERE log_t >='2017-03-19 00:00:00'
  AND log_t <='2017-03-19 23:59:59'
  AND user_id <> 'None'
  AND a_typ LIKE '%mafiabattle%'
'''
jiayuan_df, mafiabattle_df = hqls_to_dfs([jiayuan_sql, mafiabattle_sql])

jiayuan_df.to_excel('/Users/kaiqigu/Documents/h5_data/jiayuan_df.xlsx')
mafiabattle_df.to_excel('/Users/kaiqigu/Documents/h5_data/mafiabattle_df.xlsx')
