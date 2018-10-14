#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      :
Description :
'''
import settings
from utils import hql_to_df
import pandas as pd

settings.set_env('superhero_bi')

mid_info_hql = '''
SELECT *
FROM
  (SELECT uid,
          account,
          LEVEL,
          vip_level,
          zhandouli,
          regexp_replace(substr(create_time,1,10),'-','') reg_time ,
          row_number() over(partition BY account
                            ORDER BY LEVEL DESC) rn
   FROM mid_info_all
   WHERE ds = '20160905'
     AND substr(uid,1,1)='a' )a
WHERE rn = 1
'''
info_hql = '''
SELECT uid
FROM raw_info
WHERE ds ='20160905'
'''
info_df = hql_to_df(info_hql)
mid_info_df = hql_to_df(mid_info_hql)
print mid_info_df
mid_info_df['is_dau'] = mid_info_df['uid'].isin(info_df.uid.values)

not_dau_df = mid_info_df[~mid_info_df['is_dau']]
reg_03_df = not_dau_df.loc[not_dau_df.reg_time == '20160903']
reg_04_df = not_dau_df.loc[not_dau_df.reg_time == '20160904']
not_dau_df['is_03'] = not_dau_df['uid'].isin(reg_03_df.uid.values)
not_dau_df['is_04'] = not_dau_df['uid'].isin(reg_04_df.uid.values)
not_dau_df = not_dau_df.sort_values(by='reg_time',ascending=False)

columns = ['uid','account','level','vip_level','zhandouli','reg_time','is_03','is_04']
not_dau_df = not_dau_df[columns]

not_dau_df.to_excel('/Users/kaiqigu/Downloads/Excel/20160905.xlsx')







