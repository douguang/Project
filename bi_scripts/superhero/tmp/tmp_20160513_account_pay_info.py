#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : account收入数据
'''
import pandas as pd
from utils import date_range, hql_to_df
import settings

settings.set_env('superhero_bi')

start_date = '20160101'
end_date = '20160430'

# 新增account列表
new_account_df = pd.read_csv('/home/data/superhero/reg_act/dw_superhero_newaccount_20160512_all', sep='\t', header=None, index_col=False)
new_account_df = new_account_df.loc[(new_account_df[0] >= 20160101) & (new_account_df[0] <= 20160430)][[0, 1]]
new_account_df.columns = ['ds', 'account']
new_account_df['ds'] = new_account_df

# 按月汇总的充值
pay_sql = '''
select user_id, substr(ds, 5, 2) as mon, sum(order_money) as pay_rmb
from raw_paylog
where ds >= '20160101' and ds <= '20160430'
      and platform_2 != 'admin_test'
group by user_id, substr(ds, 5, 2)
'''
pay_df = hql_to_df(pay_sql)

# uid和account对应关系
uid_account_df = pd.read_csv('/home/data/superhero/log_redis/total_info_20160512', sep='\t', header=None, index_col=False)[[0, 1]]
uid_account_df.columns = ['user_id', 'account']

# gs列表
gs_df = pd.read_csv('/home/data/superhero/paylog/gs_user_20160512', sep='\t', header=None, index_col=False)
gs_set = set(gs_df[1].tolist())

uid_account_df.columns = ['user_id', 'account']
new_account_df['mon'] = new_account_df.ds.map(lambda s: str(s)[4:6])
reg_account_uid_df = uid_account_df.merge(new_account_df, on='account')

reg_mon_account_df = reg_account_uid_df.merge(pay_df, on=['user_id', 'mon'], how='left').fillna(0)

grouped_df = reg_mon_account_df.groupby('ds')
result_df = pd.concat([grouped_df.account.nunique(), grouped_df.pay_rmb.sum()], axis=1).reset_index()
result_df.to_excel('/tmp/superhero_account_pay_20160513.xlsx', index=False)
