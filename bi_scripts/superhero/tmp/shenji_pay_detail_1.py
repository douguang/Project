#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 审计 - 玩家充值明细
'''
from utils import hqls_to_dfs
import settings
import pandas as pd

settings.set_env('superhero_bi')
# 截止日期
date2 = '20160630'

sum_pay_sql = '''
SELECT uid,
       platform_2,
       sum(order_money) sum_pay,
       sum(order_coin) sum_coin,
       count(order_id) pay_num,
       min(order_time) first_order_time,
       max(order_time) last_order_time,
       count(DISTINCT to_date(order_time)) pay_day_num
FROM mid_paylog_all
WHERE ds ='20160630'
GROUP BY uid,
         platform_2
'''
info_sql ='''
SELECT uid,
       platform_2,
       min(create_time) create_time,
       max(fresh_time) last_login_time,
       sum(zuanshi) day_coin_num
FROM mid_info_all
WHERE ds ='20160630'
GROUP BY uid,
         platform_2
'''
act_sql = '''
SELECT DISTINCT login_day_num,
                         uid,
                         platform_2
FROM
  (SELECT uid,
          platform_2,
          count(ds) login_day_num
   FROM raw_act
   WHERE ds<='20160630'
   GROUP BY uid,
            platform_2) c
'''
pay_df,sum_pay_df,info_df,act_df,sum_pay_04_df = hqls_to_dfs([pay_sql,sum_pay_sql,info_sql,act_sql,sum_pay_04_sql])
# pay_df,sum_pay_df,info_df,act_df,sum_pay_04_df = hqls_to_dfs([pay_sql,sum_pay_sql,info_sql,act_sql,sum_pay_04_sql])
# sum_pay_04_df['dt'] = '201604'
# sum_pay_re_df = pd.concat([sum_pay_df,sum_pay_04_df])
# result = (pay_df.merge(info_df,on=['dt','uid','platform_2'],how='left')
#                 .merge(act_df,on=['dt','uid','platform_2'],how='left')
#                 .merge(sum_pay_re_df,on=['dt','uid','platform_2'],how='left')
#                 .fillna(0)
#                 )



