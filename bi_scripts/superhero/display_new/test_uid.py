#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : ios和qiku的测试用户
Time        : 2017.04.17
'''
import settings_dev
import pandas as pd
from utils import hqls_to_dfs
from utils import update_mysql
from utils import get_config

settings_dev.set_env('superhero_qiku')
pay_sql = '''
SELECT uid,sum(order_money) sum_money
FROM total_paylog
WHERE regexp_replace(substr(order_time, 1, 10), '-', '') <= '20170221'
  AND platform_2 = 'admin_test'
group by uid
'''
pay_new_sql = '''
SELECT uid,sum(order_money) sum_money
FROM raw_paylog
WHERE ds > '20170221'
  AND platform_2 = 'admin_test'
group by uid
'''
info_sql = '''
SELECT uid,
       nick,
       vip_level,
       level
FROM mid_info_all
WHERE ds = '20170416'
'''
pay_df, pay_new_df, info_df = hqls_to_dfs([pay_sql, pay_new_sql, info_sql])
result_df = pd.concat([pay_df, pay_new_df])
result_df = result_df.groupby('uid').sum().reset_index()
pay_result = result_df.merge(info_df, on='uid', how='left')

pay_result.to_csv('/Users/kaiqigu/Downloads/Excel/pay_result_qiku',
                  sep='\t', index=False, header=False)
