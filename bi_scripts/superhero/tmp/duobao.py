#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 一元夺宝数据
'''
from utils import hqls_to_dfs
import settings

settings.set_env('superhero_bi')
date = '20161004'

# 直接购买
buy_sql ='''
SELECT ds,sum(coin_num) buy_spend_num
FROM raw_spendlog
WHERE goods_type = 'raiders.buy_overdue_raiders'
  AND ds ='{0}'
  and substr(uid,1,1) = 'g'
group by ds
'''.format(date)
# 立即夺宝
open_sql ='''
SELECT ds,sum(coin_num) open_spend_num
FROM raw_spendlog
WHERE goods_type = 'raiders.open_raiders'
  AND ds ='{0}'
  and substr(uid,1,1) = 'g'
group by ds
'''.format(date)
buy_df,open_df = hqls_to_dfs([buy_sql,open_sql])
result_df = buy_df.merge(open_df,on='ds')
result_df['total_num'] = result_df['buy_spend_num']+result_df['open_spend_num']
columns = ['ds','buy_spend_num','open_spend_num','total_num']
result_df = result_df[columns]

result_df.to_excel('/Users/kaiqigu/Downloads/Excel/yiyuan.xlsx')


