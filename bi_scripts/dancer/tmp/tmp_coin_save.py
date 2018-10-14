#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 限时兑换数据
'''
from utils import hql_to_df, ds_add
import pandas as pd
import settings_dev
from pandas import DataFrame


settings_dev.set_env('dancer_tw')

money_after_sql = '''
with money_after_coin as(
select ds,server,user_id,vip,log_t,money_after,row_number() over(partition by ds,user_id order by log_t desc) rn
  from mid_actionlog where ds >='20160907' and ds <='20160917'
  and reverse(substring(reverse(user_id), 8)) >= 'tw0' and reverse(substring(reverse(user_id), 8)) <= 'tw2'
  and freemoney_after is null and money_after is not null
) select  ds,server,user_id ,vip,money_after from money_after_coin
'''
money_after_df = hql_to_df(money_after_sql)
freemoney_after_sql = '''
with freemoney_after as(
select ds,server,user_id ,vip,log_t,freemoney_after,row_number() over(partition by ds,user_id order by log_t desc) rn
  from mid_actionlog where ds >='20160907' and ds <='20160917'
  and reverse(substring(reverse(user_id), 8)) >= 'tw0' and reverse(substring(reverse(user_id), 8)) <= 'tw2'
  and freemoney_after is not null and money_after is null
) select ds,server,user_id ,vip,freemoney_after from freemoney_after where rn =1
'''
freemoney_after_df = hql_to_df(freemoney_after_sql)

# vip钻石存量
money_after_vip_df = money_after_df.loc[money_after_df.vip>0]
freemoney_after_vip_df = freemoney_after_df.loc[freemoney_after_df.vip>0]
result = money_after_vip_df.merge(freemoney_after_vip_df,on= ['ds','server','user_id','vip'],how='outer')
result = result.fillna(0)
result = result.groupby(['ds','server']).sum().reset_index()
result['sum_coin'] = result['money_after'] + result['freemoney_after']
columns = [ 'ds' ,'server','sum_coin']
result = result[columns]

# 大R钻石存量
money_after_vip_df = money_after_df.loc[money_after_df.vip == 0]
freemoney_after_vip_df = freemoney_after_df.loc[freemoney_after_df.vip == 0]
result_df = money_after_vip_df.merge(freemoney_after_vip_df,on= ['ds','server','user_id','vip'],how='outer')
result_df = result_df.fillna(0)
result_df = result_df.groupby(['ds','server']).sum().reset_index()
result_df['sum_coin'] = result_df['money_after'] + result_df['freemoney_after']
columns = [ 'ds' ,'server','sum_coin']
result_df = result_df[columns]

result.to_excel('/Users/kaiqigu/Downloads/Excel/vip_coin.xlsx')
result_df.to_excel('/Users/kaiqigu/Downloads/Excel/vip0_coin.xlsx')




