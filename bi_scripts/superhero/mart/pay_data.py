#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
import settings_dev
from utils import date_range
from lib.utils import run_hql

platform = 'superhero_bi'
settings_dev.set_env(platform)
# date = '20140522'
for date in date_range('20140523', '20150605'):
    try:
        pay_sql = '''
        INSERT overwrite TABLE raw_paylog partition (ds='{date}')
        SELECT order_id,
               ADMIN,
               gift_coin,
               LEVEL,
               old_coin,
               order_coin,
               order_money,
               order_time,
               platform_2,
               product_id,
               raw_data,
               reason,
               scheme_id,
               uid,
               pay_pt,
               pay_pt_2
        FROM total_paylog
        WHERE regexp_replace(substr(order_time,1,10),'-','') ='{date}'
        '''.format(date=date)
        run_hql(pay_sql, platform)
    except Exception, e:
        print e
        raise
