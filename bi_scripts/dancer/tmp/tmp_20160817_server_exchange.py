#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import settings_dev
from utils import hql_to_df, update_mysql, ds_add, date_range
from pandas import DataFrame
import pandas as pd
settings_dev.set_env('dancer_tw')
shop_sql = '''
SELECT user_id,
       a_tar
FROM mid_actionlog
WHERE a_typ = 'seven_scripture.get_reward'
  AND user_id = 'tw09267834'
  AND ds >= '20160823'
  AND return_code = ''
'''
shop_df = hql_to_df(shop_sql)
user_id, shop_id = [], []
for i in range(len(shop_df)):
    a_tar = shop_df.iloc[i,1]
    if 'mobage_id' in a_tar:
        a_tar = eval(a_tar)
        user_id.append(shop_df.iloc[i,0])
        shop_id.append(a_tar['reward_id'])
shop_buy = DataFrame({'user_id':user_id, 'shop_id':shop_id})
print shop_buy
# result_df = shop_buy.groupby('shop_id')['user_id'].agg({'user_num': lambda g: g.nunique(),'count':lambda h: h.count()}).reset_index()
# print result_df
