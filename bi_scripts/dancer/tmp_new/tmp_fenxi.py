#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
Time        : 2017.06.05
illustration:
'''
import settings_dev
import pandas as pd
# from utils import ds_add
from utils import hqls_to_dfs
# from utils import date_range

settings_dev.set_env('dancer_pub')
# 新增付费用户付费人数，付费金额
new_account_sql = '''
SELECT c.ds,
       d.register_lan_sort,
       count(DISTINCT c.account) pay_num,
       sum(order_money) AS sum_money
FROM
  (SELECT ds,
          user_id,
          order_money
   FROM raw_paylog
   WHERE platform_2 <> 'admin_test'
     AND order_id NOT LIKE '%test%') b
JOIN
  ( SELECT ds,
           user_id,
           account
   FROM parse_info)c ON b.ds = c.ds
AND b.user_id = c.user_id
JOIN
  (SELECT ds,
          account,
          register_lan_sort
   FROM mid_new_account)d ON c.ds = d.ds
AND c.account = d.account
GROUP BY c.ds,
         d.register_lan_sort
ORDER BY d.register_lan_sort,
         c.ds
'''
new_account_df = hqls_to_dfs([new_account_sql])
# 充值前5名
frist5_sql = '''
SELECT *
FROM
  (SELECT ds,
          register_lan_sort,
          account,
          sum_money,
          row_number() over(partition BY ds,register_lan_sort
                            ORDER BY sum_money DESC) AS rn
   FROM
     (SELECT b.ds,
             c.register_lan_sort,
             c.account,
             sum(order_money) AS sum_money
      FROM
        (SELECT ds,
                user_id,
                order_money
         FROM raw_paylog
         WHERE platform_2 <> 'admin_test'
           AND order_id NOT LIKE '%test%') b
      JOIN
        ( SELECT ds,
                 user_id,
                 account,
                 register_lan_sort
         FROM parse_info )c ON b.ds = c.ds
      AND b.user_id = c.user_id
      GROUP BY b.ds,
               c.register_lan_sort,
               c.account
      ORDER BY c.register_lan_sort,
               b.ds,
               c.account)d) f
WHERE rn <=5
'''
# 付费用户数，付费金额
pay_sql = '''
SELECT c.ds,
       c.register_lan_sort,
       count(DISTINCT c.account) account_num,
       sum(order_money) AS sum_money
FROM
  (SELECT ds,
          user_id,
          order_money
   FROM raw_paylog
   WHERE ds>='20170629'
     AND ds <='20170711'
     AND platform_2 <> 'admin_test'
     AND order_id NOT LIKE '%test%') b
JOIN
  ( SELECT ds,
           user_id,
           account,
           register_lan_sort
   FROM parse_info)c ON b.ds = c.ds
AND b.user_id = c.user_id
GROUP BY c.register_lan_sort,
         c.ds
ORDER BY c.register_lan_sort,
         c.ds
'''
# 当日活跃account
act_sql = '''
SELECT ds,
       register_lan_sort,
       count(account)
FROM parse_info
WHERE ds >='20170629'
  AND ds <='20170711'
GROUP BY ds,
         register_lan_sort
ORDER BY register_lan_sort,
         ds
'''





