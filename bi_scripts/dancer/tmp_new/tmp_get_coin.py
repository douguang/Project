#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 元宝消耗数据
Time        : 2017.07.14
illustration: 购买体力：shop.shop_buy  shop_id=1001
'''
import settings_dev
import pandas as pd
from utils import hqls_to_dfs

settings_dev.set_env('dancer_mul')
spend_sql = '''
SELECT a.ds,
       b.server,
       b.vip,
       a.goods_type,
       sum(a.sum_coin) AS sum_spend_coin ,
       sum(b.coin_num) AS save_coin
FROM
  (SELECT ds,
          user_id,
          goods_type,
          sum(coin_num) AS sum_coin
   FROM raw_spendlog
   GROUP BY ds,
            user_id,
            goods_type)a
JOIN
  (SELECT ds ,
          user_id,
          vip,
          reverse(substr(reverse(user_id),8)) AS server ,
          free_coin+charge_coin AS coin_num
   FROM parse_info)b ON a.ds = b.ds
AND a.user_id = b.user_id
where a.ds = '20170629'
and b.server = 'pm1'
GROUP BY a.ds,
         b.server,
         b.vip,
         a.goods_type
'''
