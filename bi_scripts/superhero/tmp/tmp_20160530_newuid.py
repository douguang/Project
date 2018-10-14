#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 留存率
'''
import pandas as pd
from pandas import DataFrame
from utils import date_range, hqls_to_dfs,ds_add
import settings

settings.set_env('superhero_bi')
date1 = '20160603'
date2 = '20160702'
name ='g515'

# substring(uid, 1,1) server
reg_sql = "select ds,uid,reverse(substring(reverse(uid), 8)) server from raw_reg where ds>='{0}' and ds <= '{1}' ".format(date1,date2)
act_sql = "select ds,uid,reverse(substring(reverse(uid), 8)) server,vip_level from raw_info where ds>='{0}' and ds <= '{1}' ".format(date1,date2)
pay_sql = "select ds,uid,reverse(substring(reverse(uid), 8)) server,order_money from raw_paylog where ds>='{0}' and ds <= '{1}' ".format(date1,date2)

reg_df,act_df,pay_df = hqls_to_dfs([reg_sql,act_sql,pay_sql])

reg_df = reg_df[reg_df['server'] == name]
act_df = act_df[act_df['server'] == name]
pay_df = pay_df[pay_df['server'] == name]

# date1 = ds_add(date1,-1)
# for i in range(0,10):
while date1 <= date2:
    print date1
    # vip活跃人数
    vip_data = act_df[act_df['vip_level']>0]
    reg_data = reg_df[reg_df['ds'] == date1]
    reg_data['is_vip_uid'] = reg_data['uid'].isin(vip_data.uid.values)
    reg_data = reg_data[reg_data['is_vip_uid']]
    vip_uid = reg_data.count().to_frame().T['uid']

    # 非VIP活跃人数
    vip0_data = act_df[act_df['vip_level'] == 0]
    reg_data = reg_df[reg_df['ds'] == date1]
    reg_data['is_vip0_uid'] = reg_data['uid'].isin(vip0_data.uid.values)
    reg_data = reg_data[reg_data['is_vip0_uid']]
    vip0_uid = reg_data.count().to_frame().T['uid']

    # 充值人数、充值金额
    reg_data = reg_df[reg_df['ds'] == date1]
    pay_df['is_reg_uid'] = pay_df['uid'].isin(reg_data.uid.values)
    pay_data = pay_df[pay_df['is_reg_uid']]
    pay_num = pay_data.drop_duplicates(['uid'])
    pay_num = pay_num.count().to_frame().T['uid']
    pay_money = pay_data.sum().T['order_money']

    # 充值6元人数
    pay6_data = pay_data.groupby(['uid']).sum().reset_index()
    pay6_data = pay6_data[pay6_data['order_money'] == 6]
    pay6_num = pay6_data.count().to_frame().T['uid']

    # 新增人数
    reg_data = reg_df[reg_df['ds'] == date1]
    reg_num = reg_data.count().to_frame().T['uid']

    dau2_act = act_df[act_df['ds'] == ds_add(date1,1)]
    dau3_act = act_df[act_df['ds'] == ds_add(date1,2)]
    dau7_act = act_df[act_df['ds'] == ds_add(date1,6)]
    reg_data['is_dau2'] = reg_data['uid'].isin(dau2_act.uid.values)
    reg_data['is_dau3'] = reg_data['uid'].isin(dau3_act.uid.values)
    reg_data['is_dau7'] = reg_data['uid'].isin(dau7_act.uid.values)
    dau2_data = reg_data[reg_data['is_dau2']]
    dau2_data = dau2_data.drop_duplicates(['uid'])
    dau2_data = dau2_data.count().to_frame().T['uid']

    dau3_data = reg_data[reg_data['is_dau3']]
    dau3_data = dau3_data.drop_duplicates(['uid'])
    dau3_data = dau3_data.count().to_frame().T['uid']

    dau7_data = reg_data[reg_data['is_dau7']]
    dau7_data = dau7_data.drop_duplicates(['uid'])
    dau7_data = dau7_data.count().to_frame().T['uid']

    data = {'ds':[date1],'vip_uid':[vip_uid[0]],'vip0_uid':[vip0_uid[0]],'pay_num':[pay_num[0]],'pay_money':[pay_money],'pay6_num':[pay6_num[0]],'reg_num':[reg_num[0]],'dau2_data':[dau2_data[0]],'dau3_data':[dau3_data[0]],'dau7_data':[dau7_data[0]]}
    result = DataFrame(data)
    columns = ['ds','vip_uid','vip0_uid','pay_num','pay_money','pay6_num','reg_num','dau2_data','dau3_data','dau7_data']
    result = result[columns]

    if date1 == '20160603':
            result_data = result
    else:
        result_data = pd.concat([result_data,result])
    date1 = ds_add(date1,1)

print result_data
result_data = result_data.fillna(0)

result_data.to_excel('/Users/kaiqigu/Downloads/Excel/g515.xlsx')
