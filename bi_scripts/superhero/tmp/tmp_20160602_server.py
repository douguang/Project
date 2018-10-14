#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 新服account数据
'''
import pandas as pd
from pandas import DataFrame
from utils import date_range, hqls_to_dfs,ds_add
import settings

settings.set_env('superhero_bi')
# settings.set_env('superhero_qiku')
# name ='g497'
name ='g511'
date1 = '20160520'
date2 = '20160601'
# use superhero_bi;
# invalidate metadata;
# account 非vip活跃人数
vip0_sql = '''
select ds,count(distinct account) vip0 from raw_info
where vip_level = 0 and reverse(substring(reverse(uid), 8)) = '{name}' and ds >='{date1}' and ds <= '{date2}' group by ds order by ds
'''.format(**{
    'name':name,
    'date1':date1,
    'date2':date2,
    })

# --account vip活跃人数
vip_sql = '''
select ds,count(distinct account) vip from raw_info
where vip_level > 0 and reverse(substring(reverse(uid), 8)) = '{name}' and ds >='{date1}' and ds <= '{date2}' group by ds order by ds
'''.format(**{
    'name':name,
    'date1':date1,
    'date2':date2,
    })

# 充值金额
pay_sql ='''
select ds,sum(order_money) sum_money from raw_paylog
where reverse(substring(reverse(user_id), 8)) = '{name}' and ds >='{date1}' and ds <= '{date2}' group by ds order by ds
'''.format(**{
    'name':name,
    'date1':date1,
    'date2':date2,
    })


# 充值人数
paynum_sql ='''
select ds,count(distinct account) as pay_num from raw_info
left semi join
(select user_id,ds from raw_paylog where reverse(substring(reverse(user_id), 8)) = '{name}'
            and ds >='{date1}' and ds <= '{date2}'
)a
on raw_info.uid = a.user_id
and raw_info.ds = a.ds
where  raw_info.ds >='{date1}' and raw_info.ds <= '{date2}' and reverse(substring(reverse(raw_info.uid), 8)) = '{name}'
group by ds
order by ds
'''.format(**{
    'name':name,
    'date1':date1,
    'date2':date2,
    })

# 充值6元人数
pay6_num_sql = '''
select ds,count(distinct account) as pay6_num from raw_info
left semi join
(select ds,user_id from
(
    select ds,user_id,sum(order_money) sum_money
    from raw_paylog
    where  reverse(substring(reverse(user_id), 8)) = '{name}'
    and ds >='{date1}' and ds <= '{date2}' group by ds,user_id
)b
where sum_money=6
)a
on raw_info.uid = a.user_id
and raw_info.ds = a.ds
where  raw_info.ds >='{date1}' and raw_info.ds <= '{date2}'  and reverse(substring(reverse(raw_info.uid), 8)) = '{name}'
group by ds
order by ds
'''.format(**{
    'name':name,
    'date1':date1,
    'date2':date2,
    })

# 新增人数
reg_num_sql ='''
select ds,count(distinct account) as reg_num from raw_info
left semi join
(select uid,ds from raw_reg where reverse(substring(reverse(uid), 8)) = '{name}'
            and ds >='{date1}' and ds <= '{date2}'
)a
on raw_info.uid = a.uid
and raw_info.ds = a.ds
where  raw_info.ds >='{date1}' and raw_info.ds <= '{date2}' and reverse(substring(reverse(raw_info.uid), 8)) = '{name}'
group by ds
order by ds
'''.format(**{
    'name':name,
    'date1':date1,
    'date2':date2,
    })

vip_df,vip0_df,pay_df,paynum_df,pay6_num_df,reg_num_df = hqls_to_dfs([vip_sql,vip0_sql,pay_sql,paynum_sql,pay6_num_sql,reg_num_sql])

result =  (vip_df
                .merge(vip0_df,on=['ds'],how='outer')
                .merge(pay_df,on=['ds'],how='outer')
                .merge(paynum_df,on=['ds'],how='outer')
                .merge(pay6_num_df,on=['ds'],how='outer')
                .merge(reg_num_df,on=['ds'],how='outer')
            )
columns = ['ds','vip','vip0','pay_num','sum_money','pay6_num','reg_num']
result = result[columns]
result.to_excel('/Users/kaiqigu/Downloads/Excel/g512.xlsx')




# reg_sql = "select ds,uid,reverse(substring(reverse(uid), 8)) server from raw_reg where ds>='{0}' and ds <= '{1}' ".format(date1,date2)
# act_sql = "select ds,uid,account,reverse(substring(reverse(uid), 8)) server,vip_level from raw_info where ds>='{0}' and ds <= '{1}' ".format(date1,date2)
# pay_sql = "select ds,user_id,reverse(substring(reverse(user_id), 8)) server,order_money from raw_paylog where ds>='{0}' and ds <= '{1}' ".format(date1,date2)

# reg_df,act_df,pay_df = hqls_to_dfs([reg_sql,act_sql,pay_sql])

# reg_df = reg_df[reg_df['server'] == name]
# act_df = act_df[act_df['server'] == name]
# pay_df = pay_df[pay_df['server'] == name]

# date1 = '20160519'
# for i in range(0,13):
#     date1 = ds_add(date1,1)
#     print date1

#     # 新增人数
#     reg_data = reg_df[reg_df['ds'] == date1]
#     reg_num = reg_data.count().to_frame().T['uid']

#     dau2_act = act_df[act_df['ds'] == ds_add(date1,1)]
#     dau3_act = act_df[act_df['ds'] == ds_add(date1,2)]
#     dau7_act = act_df[act_df['ds'] == ds_add(date1,6)]
#     dau2_act['is_dau2'] = dau2_act['uid'].isin(reg_data.uid.values)
#     dau3_act['is_dau3'] = dau3_act['uid'].isin(reg_data.uid.values)
#     dau7_act['is_dau7'] = dau7_act['uid'].isin(reg_data.uid.values)

#     dau2_data = dau2_act[dau2_act['is_dau2']]
#     dau2_data = dau2_data.drop_duplicates(['account'])
#     dau2_data = dau2_data.count().to_frame().T['account']
#     # dau2_data = dau2_data/reg_num

#     dau3_data = dau3_act[dau3_act['is_dau3']]
#     dau3_data = dau3_data.drop_duplicates(['account'])
#     dau3_data = dau3_data.count().to_frame().T['account']
#     # dau3_data = dau3_data/reg_num

#     dau7_data = dau7_act[dau7_act['is_dau7']]
#     dau7_data = dau7_data.drop_duplicates(['account'])
#     dau7_data = dau7_data.count().to_frame().T['account']
#     # dau7_data = dau7_data/reg_num

#     data = {'ds':[date1],'dau2_data':[dau2_data[0]],'dau3_data':[dau3_data[0]],'dau7_data':[dau7_data[0]]}
#     result = DataFrame(data)
#     columns = ['ds','dau2_data','dau3_data','dau7_data']
#     result = result[columns]

#     if date1 == '20160520':
#             result_data = result
#     else:
#         result_data = pd.concat([result_data,result])

# result_data.to_excel('/Users/kaiqigu/Downloads/Excel/g512_rata.xlsx')
