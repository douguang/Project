#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
# 钻石商店:shop.shop_buy {u'count': u'1', u'use': u'1', u'shop_id': u'1001', u'mobage_id': u'kvgames_10119627'}
# 传奇商店:shop.spark_shop_buy {u'pos_id': u'5', u'mobage_id': u'androidfb_10154425548144772'}
# 功勋商店:shop.exploit_shop_buy {u'count': u'1', u'shop_id': u'5', u'mobage_id': u'kvgames_10119586'}   error_45
# 声望商店:shop.pvp_shop_buy  {u'pos_id': u'7', u'mobage_id': u'androidfb_1631661027146596'}
# 史诗商店:shop.epic_shop_buy {u'pos_id': u'8', u'mobage_id': u'androidfb_1109008589164826'}
'''
import settings_dev
from utils import hql_to_df, update_mysql, ds_add, date_range
from pandas import DataFrame
import pandas as pd
settings_dev.set_env('dancer_tw')
# 钻石商店
print 'shop_buy'
shop_sql = '''
SELECT ds,
       user_id,
       a_typ,
       a_tar
FROM mid_actionlog
WHERE ds >= '20160808'
  AND ds <= '20160814'
  AND a_typ = 'shop.shop_buy'
  AND return_code = ''
  AND user_id IN
    (SELECT user_id
     FROM mid_info_all
     WHERE ds = '20160814'
       AND to_date(reg_time) IN ('2016-08-08',
                                 '2016-08-09',
                                 '2016-08-10'))
  AND user_id IN
    (SELECT user_id
     FROM raw_paylog
     WHERE platform_2 != 'admin_test')
'''
shop_df = hql_to_df(shop_sql)
ds, user_id, a_typ, count, shop_id = [], [], [], [], []
for i in range(len(shop_df)):
    a_tar = shop_df.iloc[i,3]
    if 'mobage_id' in a_tar:
        a_tar = eval(a_tar)
        ds.append(shop_df.iloc[i,0])
        user_id.append(shop_df.iloc[i,1])
        a_typ.append(shop_df.iloc[i,2])
        count.append(a_tar['count'])
        shop_id.append(a_tar['shop_id'])
shop_buy = DataFrame({'ds':ds, 'user_id':user_id, 'a_typ':a_typ, 'shop_id':shop_id, 'count':count})
print shop_buy

# 传奇商店
print 'spark_shop_buy'
spark_sql = '''
SELECT ds,
       user_id,
       a_typ,
       a_tar
FROM mid_actionlog
WHERE ds >= '20160808'
  AND ds <= '20160814'
  AND a_typ = 'shop.spark_shop_buy'
  AND return_code = ''
  AND user_id IN
    (SELECT user_id
     FROM mid_info_all
     WHERE ds = '20160814'
       AND to_date(reg_time) IN ('2016-08-08',
                                 '2016-08-09',
                                 '2016-08-10'))
  AND user_id IN
    (SELECT user_id
     FROM raw_paylog
     WHERE platform_2 != 'admin_test')
'''
spark_df = hql_to_df(spark_sql)
ds, user_id, a_typ, shop_id = [], [], [], []
for i in range(len(spark_df)):
    a_tar = spark_df.iloc[i,3]
    if 'mobage_id' in a_tar:
        a_tar = eval(a_tar)
        ds.append(spark_df.iloc[i,0])
        user_id.append(spark_df.iloc[i,1])
        a_typ.append(spark_df.iloc[i,2])
        shop_id.append(a_tar['pos_id'])
spark_shop_buy = DataFrame({'ds':ds, 'user_id':user_id, 'a_typ':a_typ, 'shop_id':shop_id})
print spark_shop_buy

# 功勋商店
print 'exploit_shop_buy'
exploit_sql = '''
SELECT ds,
       user_id,
       a_typ,
       a_tar
FROM mid_actionlog
WHERE ds >= '20160808'
  AND ds <= '20160814'
  AND a_typ = 'shop.exploit_shop_buy'
  AND return_code = ''
  AND user_id IN
    (SELECT user_id
     FROM mid_info_all
     WHERE ds = '20160814'
       AND to_date(reg_time) IN ('2016-08-08',
                                 '2016-08-09',
                                 '2016-08-10'))
  AND user_id IN
    (SELECT user_id
     FROM raw_paylog
     WHERE platform_2 != 'admin_test')
'''
exploit_df = hql_to_df(exploit_sql)
ds, user_id, a_typ, count, shop_id = [], [], [], [], []
for i in range(len(exploit_df)):
    a_tar = exploit_df.iloc[i,3]
    if 'mobage_id' in a_tar:
        a_tar = eval(a_tar)
        ds.append(exploit_df.iloc[i,0])
        user_id.append(exploit_df.iloc[i,1])
        a_typ.append(exploit_df.iloc[i,2])
        count.append(a_tar['count'])
        shop_id.append(a_tar['shop_id'])
exploit_shop_buy = DataFrame({'ds':ds, 'user_id':user_id, 'a_typ':a_typ, 'shop_id':shop_id, 'count':count})
print exploit_shop_buy

# 声望商店
print 'pvp_shop_buy'
pvp_sql = '''
SELECT ds,
       user_id,
       a_typ,
       a_tar
FROM mid_actionlog
WHERE ds >= '20160808'
  AND ds <= '20160814'
  AND a_typ = 'shop.pvp_shop_buy'
  AND return_code = ''
  AND user_id IN
    (SELECT user_id
     FROM mid_info_all
     WHERE ds = '20160814'
       AND to_date(reg_time) IN ('2016-08-08',
                                 '2016-08-09',
                                 '2016-08-10'))
  AND user_id IN
    (SELECT user_id
     FROM raw_paylog
     WHERE platform_2 != 'admin_test')
'''
pvp_df = hql_to_df(pvp_sql)
ds, user_id, a_typ, shop_id = [], [], [], []
for i in range(len(pvp_df)):
    a_tar = pvp_df.iloc[i,3]
    if 'mobage_id' in a_tar:
        a_tar = eval(a_tar)
        ds.append(pvp_df.iloc[i,0])
        user_id.append(pvp_df.iloc[i,1])
        a_typ.append(pvp_df.iloc[i,2])
        shop_id.append(a_tar['pos_id'])
pvp_shop_buy = DataFrame({'ds':ds, 'user_id':user_id, 'a_typ':a_typ, 'shop_id':shop_id})
print pvp_shop_buy

writer = pd.ExcelWriter('/Users/kaiqigu/Documents/dancer/tmp_20160816_shop.xlsx')
shop_buy.to_excel(writer,'shop_buy')
spark_shop_buy.to_excel(writer,'spark_shop_buy')
exploit_shop_buy.to_excel(writer,'exploit_shop_buy')
pvp_shop_buy.to_excel(writer,'pvp_shop_buy')
