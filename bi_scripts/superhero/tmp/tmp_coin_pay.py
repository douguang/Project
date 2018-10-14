#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 付费VIP用户钻石存量（vt）
'''
import settings
from utils import hqls_to_dfs, update_mysql, ds_add
import pandas as pd

settings.set_env('superhero_vt')
start_date = '20160801'
end_date = '20160814'

info_sql = '''
SELECT ds,
       count(distinct uid) user_num,
       sum(zuanshi) sum_coin
FROM raw_info
WHERE ds >= '20160801'
  AND ds <='20160814'
  and vip_level > 0
group by ds
order by ds
'''.format(start_date=start_date,end_date = end_date)

# pay_result_df.to_excel('/Users/kaiqigu/Downloads/Excel/pay_coin_cunliang.xlsx')
