#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
import settings
from utils import hql_to_df, update_mysql, date_range
import pandas as pd

settings.set_env('superhero_bi')
sql = '''
select a.ds,a.uid,nick,product_id,order_coin,order_money,a.platform_2,order_id,order_time,guan,reason_re from
(select ds,uid,product_id,order_coin,order_money,platform_2,order_id,order_time
,case when platform_2 = 'admin_test' then '1' else '0' end guan
,case when reason is null then '' else reason end reason_re
from raw_paylog where ds >= '20161101' and ds<='20161130')a
join
(select uid,nick from mid_info_all where ds ='20161130' and platform_2 in ('duoku','91'))b
on a.uid = b.uid
order by a.ds,a.uid
'''
df = hql_to_df(sql)

df.to_csv('/Users/kaiqigu/Documents/Excel/pay_detail', sep = '\t', index = False, header = False)


