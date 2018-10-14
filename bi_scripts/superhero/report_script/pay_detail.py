#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author  : Dong Junshuang
Software: Sublime Text
Time    : 20170307
Description :  每月 - 渠道充值明细
七酷-混服的充值渠道：'qiku','msdk_wechat','msdk_mpqq'
'''
from utils import hql_to_df
# from utils import ds_add
import settings_dev

settings_dev.set_env('superhero_bi')
start_date = '20170401'
end_date = '20170430'

pay_sql = '''
SELECT a.ds,
       a.uid,
       nick,
       product_id,
       order_coin,
       order_money,
       a.platform_2,
       order_id,
       order_time,
       guan,
       reason_re
FROM
  (SELECT ds,
          uid,
          product_id,
          order_coin,
          order_money,
          platform_2,
          order_id,
          order_time ,
          CASE
              WHEN platform_2 = 'admin_test' THEN '1'
              ELSE '0'
          END guan ,
          CASE
              WHEN reason IS NULL THEN ''
              ELSE reason
          END reason_re
   FROM raw_paylog
   WHERE ds >= '{start_date}'
     AND ds<='{end_date}'
     -- AND platform_2 in ('qiku','msdk_wechat','msdk_mpqq')
     AND platform_2 = 'le8'
     )a
JOIN
  (SELECT uid,
          nick
   FROM mid_info_all
   WHERE ds ='{end_date}'
     )b ON a.uid = b.uid
ORDER BY a.ds,
         a.uid
'''.format(start_date=start_date, end_date=end_date)
pay_df = hql_to_df(pay_sql)

pay_df.to_csv('/Users/kaiqigu/Documents/report/pay_df',
              sep='|', index=False, header=False)
