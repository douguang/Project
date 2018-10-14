#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 官混服LTV
'''
from utils import hql_to_df, ds_add, timestamp_to_string, date_range, hqls_to_dfs
import settings_dev
import pandas as pd

settings_dev.set_env('wn_beta')
reg_sql = '''
SELECT DISTINCT user_id
FROM mid_wn_actionlog
WHERE platform = 'uc'
  AND ds = '20160607'
'''
print reg_sql
act_sql = '''
SELECT DISTINCT user_id
FROM raw_wn_actionlog
WHERE platform = 'uc'
  AND ds >= '20160619'
'''
print act_sql
card_sql = '''
SELECT user_id,
       log_t,
       p_1,
       p_2,
       p_3,
       p_4,
       p_5,
       p_6,
       p_7,
       p_8,
       p_9
FROM
  (SELECT split(body.a_usr, '@')[1] as user_id,
          log_t,
          split(align, '_')[0] AS p_1,
          split(align, '_')[1] AS p_2,
          split(align, '_')[2] AS p_3,
          split(align, '_')[3] AS p_4,
          split(align, '_')[4] AS p_5,
          split(align, '_')[5] AS p_6,
          split(align, '_')[6] AS p_7,
          split(align, '_')[7] AS p_8,
          split(align, '_')[8] AS p_9
   FROM raw_wn_actionlog LATERAL VIEW json_tuple(body.a_tar,'align') j_tab AS align
   WHERE param5 = 'uc'
     AND body.a_typ = 'cards.cards_data_mix') t1
JOIN
  (SELECT split(body.a_usr, '@')[1] as user_id,
          max(log_t) AS log_t
   FROM raw_wn_actionlog
   WHERE param5 = 'uc'
     AND body.a_typ = 'cards.cards_data_mix'
  GROUP BY body.a_usr) t2 ON t1.user_id = t2.user_id
AND t1.log_t = t2.log_t'''
reg_df, act_df = hqls_to_dfs([reg_sql, act_sql])
card_df = hql_to_df(card_sql, 'hive')
print reg_df
user_df = (reg_df.merge(act_df,
                         how='inner').merge(card_df,
                                            on=['user_id'],
                                            how='inner'))
print user_df
