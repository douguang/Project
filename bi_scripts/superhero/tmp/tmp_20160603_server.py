#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 新服account数据
'''
import pandas as pd
from pandas import DataFrame
from utils import date_range, hqls_to_dfs,ds_add,hql_to_df
import settings

settings.set_env('superhero_bi')
# settings.set_env('superhero_qiku')
# name ='g497'
name ='g512'
date1 = '20160520'
date2 = '20160601'
# use superhero_bi;
# invalidate metadata;
# account 非vip活跃人数 vip活跃人数
info_sql = '''
select ds,account,uid,reverse(substring(reverse(uid), 8)) server,vip_level from raw_info
where ds >='{date1}' and ds <= '{date2}'
'''.format(**{
    'date1':date1,
    'date2':date2,
    })

pay_sql ='''
select ds,user_id,order_money,reverse(substring(reverse(user_id), 8)) server from raw_paylog
where ds >='{date1}' and ds <= '{date2}'
'''.format(**{
    'date1':date1,
    'date2':date2,
    })

reg_sql = '''
select ds,uid,reverse(substring(reverse(uid), 8)) server from raw_reg
where  ds >='{date1}' and ds <= '{date2}'
'''.format(**{
    'date1':date1,
    'date2':date2,
    })

info_df,pay_df,reg_df = hqls_to_dfs([info_sql,pay_sql,reg_sql])
# reg_df = hql_to_df(reg_sql)

g511_info_df = info_df[info_df['server'] == name ]
other_info_df = info_df[info_df['server'] != name ]
pay_df = pay_df[pay_df['server'] == name ]
reg_df = reg_df[reg_df['server'] == name ]

g511_info_df['is_new_account'] = g511_info_df['account'].isin(other_info_df.account.values)
# 新增账户
new_account_df = g511_info_df[~g511_info_df['is_new_account']]
vip0_df = new_account_df[new_account_df['vip_level'] == 0]
vipn_df = new_account_df[new_account_df['vip_level'] > 0]
pay_df['is_new_account'] = pay_df['user_id'].isin(new_account_df.uid.values)
pay_df = pay_df[pay_df['is_new_account']]
reg_df['is_new_account'] = reg_df['uid'].isin(new_account_df.uid.values)
reg_df = reg_df[reg_df['is_new_account']]
# vip0
vip0_df_result = vip0_df.groupby('ds').count().reset_index().loc[:,['ds','account']].rename(columns={'account':'vip0'})
# vipn
vipn_df_result = vipn_df.groupby('ds').count().reset_index().loc[:,['ds','account']].rename(columns={'account':'vipn'})
# pay_money
pay_money_result = pay_df.groupby('ds').sum().reset_index().loc[:,['ds','order_money']].rename(columns={'order_money':'pay_money'})
# pay_num
pay_num_result = pay_df.groupby('ds').count().reset_index().loc[:,['ds','order_money']].rename(columns={'order_money':'pay_num'})
# pay6_num
pay6_df = pay_df.groupby(['ds','user_id']).sum().reset_index()
pay6_df = pay6_df[pay6_df['order_money'] == 6]
pay6_df_result = pay6_df.groupby('ds').count().reset_index().loc[:,['ds','order_money']].rename(columns={'order_money':'pay6_num'})

# reg_num
reg_df_result = reg_df.groupby('ds').count().reset_index().loc[:,['ds','uid']].rename(columns={'uid':'reg_num'})

result =  (vipn_df_result
                .merge(vip0_df_result,on=['ds'])
                .merge(pay_money_result,on=['ds'])
                .merge(pay_num_result,on=['ds'])
                .merge(pay6_df_result,on=['ds'])
                .merge(reg_df_result,on=['ds'])
            )
columns = ['ds','vipn','vip0','pay_num','pay_money','pay6_num','reg_num']
result = result[columns]
result.to_excel('/Users/kaiqigu/Downloads/Excel/g512.xlsx')

# reg_sql = "select ds,uid,reverse(substring(reverse(uid), 8)) server from raw_reg where ds>='{0}' and ds <= '{1}' ".format(date1,date2)
# act_sql = "select ds,uid,account,reverse(substring(reverse(uid), 8)) server,vip_level from raw_info where ds>='{0}' and ds <= '{1}' ".format(date1,date2)
# pay_sql = "select ds,user_id,reverse(substring(reverse(user_id), 8)) server,order_money from raw_paylog where ds>='{0}' and ds <= '{1}' ".format(date1,date2)

# reg_df,act_df,pay_df = hqls_to_dfs([reg_sql,act_sql,pay_sql])

# reg_df = reg_df[reg_df['server'] == name]
# act_df = act_df[act_df['server'] == name]
# pay_df = pay_df[pay_df['server'] == name]

date1 = '20160519'
for i in range(0,13):
    date1 = ds_add(date1,1)
    print date1

    # 新增人数
    reg_data = reg_df[reg_df['ds'] == date1]
    reg_num = reg_data.count().to_frame().T['uid']

    dau2_act = new_account_df[new_account_df['ds'] == ds_add(date1,1)]
    dau3_act = new_account_df[new_account_df['ds'] == ds_add(date1,2)]
    dau7_act = new_account_df[new_account_df['ds'] == ds_add(date1,6)]
    dau2_act['is_dau2'] = dau2_act['uid'].isin(reg_data.uid.values)
    dau3_act['is_dau3'] = dau3_act['uid'].isin(reg_data.uid.values)
    dau7_act['is_dau7'] = dau7_act['uid'].isin(reg_data.uid.values)

    dau2_data = dau2_act[dau2_act['is_dau2']]
    dau2_data = dau2_data.drop_duplicates(['account'])
    dau2_data = dau2_data.count().to_frame().T['account']
    # dau2_data = dau2_data/reg_num

    dau3_data = dau3_act[dau3_act['is_dau3']]
    dau3_data = dau3_data.drop_duplicates(['account'])
    dau3_data = dau3_data.count().to_frame().T['account']
    # dau3_data = dau3_data/reg_num

    dau7_data = dau7_act[dau7_act['is_dau7']]
    dau7_data = dau7_data.drop_duplicates(['account'])
    dau7_data = dau7_data.count().to_frame().T['account']
    # dau7_data = dau7_data/reg_num

    data = {'ds':[date1],'dau2_data':[dau2_data[0]],'dau3_data':[dau3_data[0]],'dau7_data':[dau7_data[0]]}
    result = DataFrame(data)
    columns = ['ds','dau2_data','dau3_data','dau7_data']
    result = result[columns]

    if date1 == '20160520':
            result_data = result
    else:
        result_data = pd.concat([result_data,result])

result_data.to_excel('/Users/kaiqigu/Downloads/Excel/g512_rata.xlsx')
