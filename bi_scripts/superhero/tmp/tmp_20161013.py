#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
from utils import hql_to_df,ds_add
import settings
from pandas import DataFrame

settings.set_env('superhero_bi')

reg_dates = '20160927'
pp = 'a'
reg_sql = '''
SELECT reg_ds,
       uid
FROM
  ( SELECT ds AS reg_ds,
           uid
   FROM raw_reg
   WHERE ds = '{reg_dates}'
     AND substr(uid,1,1) = '{pp}' )a LEFT semi
JOIN
  (SELECT ds ,
          uid
   FROM raw_info
   WHERE ds = '{reg_dates}'
     AND substr(uid,1,1) = '{pp}')b ON a.reg_ds = b.ds
AND a.uid = b.uid
'''.format(reg_dates=reg_dates,pp=pp)
# reg_sql = '''
# SELECT reg_ds,
#        uid
# FROM
#   ( SELECT ds AS reg_ds,
#            uid
#    FROM raw_reg
#    WHERE ds = '{reg_dates}')a LEFT semi
# JOIN
#   (SELECT distinct ds ,
#           uid
#    FROM raw_action_log
#    WHERE ds = '{reg_dates}')b ON a.reg_ds = b.ds
# AND a.uid = b.uid
# '''.format(reg_dates=reg_dates)
reg_df = hql_to_df(reg_sql)
info_sql = '''
select ds,uid from raw_info
where ds >='{reg_dates}'
'''.format(reg_dates=reg_dates)
info_df = hql_to_df(info_sql)

info_df['is_reg'] = info_df['uid'].isin(reg_df.uid.values)
result = info_df[info_df['is_reg']]

result_df = result.loc[result.ds == ds_add(reg_dates,6)]






