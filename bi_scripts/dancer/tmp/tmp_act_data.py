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
act_sql = '''
select ds,server,user_id
FROM mid_actionlog
WHERE ds >='20160907'
  AND ds <='20160917'
  AND server >= 'tw0'
  AND server <= 'tw2'
group by ds,server,user_id
'''
act_df = hql_to_df(act_sql)
# pay_sql ='''
# select ds,reverse(substring(reverse(user_id), 8)) AS server,sum(order_money),count(distinct user_id)
# from raw_paylog
# where ds >='20160907' and ds <='20160917'
# and reverse(substring(reverse(user_id), 8)) >= 'tw0' and reverse(substring(reverse(user_id), 8)) <= 'tw2'
# group by ds, reverse(substring(reverse(user_id), 8))
# order by ds, reverse(substring(reverse(user_id), 8))
# '''
# pay_df = hql_to_df(pay_sql)
pay_uid_sql ='''
select ds,reverse(substring(reverse(user_id), 8)) AS server,user_id,sum(order_money) sum_money
from raw_paylog
where ds >='20160907' and ds <='20160917'
and reverse(substring(reverse(user_id), 8)) >= 'tw0' and reverse(substring(reverse(user_id), 8)) <= 'tw2'
group by ds, reverse(substring(reverse(user_id), 8)),user_id
order by ds, reverse(substring(reverse(user_id), 8)),user_id
'''
pay_uid_df = hql_to_df(pay_uid_sql)

# 活跃人数
act_user_df = act_df.groupby(['ds','server']).count().reset_index().rename(columns = {'user_id':'uid_num'})
# 活跃付费用户人数
act_df['ds_uid'] = act_df['ds']+act_df['user_id']
pay_uid_df['ds_uid'] = pay_uid_df['ds']+pay_uid_df['user_id']
act_df['is_pay'] = act_df['ds_uid'].isin(pay_uid_df.ds_uid.values)
act_pay_df = act_df[act_df['is_pay']]
act_pay_num_df = act_pay_df.groupby(['ds','server']).count().user_id.reset_index().rename(columns = {'user_id':'act_pay_num'})
# 充值总额
pay_uid_money_df = pay_uid_df.groupby(['ds','server']).sum().sum_money.reset_index()
# 充值人数
pay_uid_num_df = pay_uid_df.groupby(['ds','server']).count().user_id.reset_index().rename(columns = {'user_id':'pay_num'})

result = (act_user_df.merge(act_pay_num_df,on=['ds','server'])
    .merge(pay_uid_money_df,on=['ds','server'])
    .merge(pay_uid_num_df,on=['ds','server'])
    )

result.to_excel('/Users/kaiqigu/Downloads/Excel/info_data.xlsx')
