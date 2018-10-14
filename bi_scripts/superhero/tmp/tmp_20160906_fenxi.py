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

info_hql = '''
SELECT ds,
       uid,
       account,
       LEVEL,
       vip_level,
       zhandouli,
       regexp_replace(substr(create_time,1,10),'-','') reg_time
FROM raw_info
WHERE ds IN ('20160904',
             '20160905')
  AND substr(uid,1,1)='a'
'''
info_df = hql_to_df(info_hql)
reg_sql = '''
SELECT ds,
       uid
FROM raw_reg
WHERE ds IN ('20160904',
             '20160903')
  AND substr(uid,1,1)='a'
'''
reg_df = hql_to_df(reg_sql)

info_04_df = info_df.loc[info_df.ds == '20160904']
info_05_df = info_df.loc[info_df.ds == '20160905']
reg_03_df = reg_df.loc[reg_df.ds == '20160903']
reg_04_df = reg_df.loc[reg_df.ds == '20160904']

info_04_df['is_dau'] = info_04_df['account'].isin(info_05_df.account.values)
not_dau_df = info_04_df[~info_04_df['is_dau']]
not_dau_df['is_03'] = not_dau_df['uid'].isin(reg_03_df.uid.values)
not_dau_df['is_04'] = not_dau_df['uid'].isin(reg_04_df.uid.values)

not_dau_df = not_dau_df.sort_values(by='reg_time',ascending=False)

columns = ['uid','account','level','vip_level','zhandouli','reg_time','is_03','is_04']
not_dau_df = not_dau_df[columns]

# not_dau_df.to_excel('/Users/kaiqigu/Downloads/Excel/20160905.xlsx')







