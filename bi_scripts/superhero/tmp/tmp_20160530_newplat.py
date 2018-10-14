#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
import pandas as pd
from utils import date_range, hqls_to_dfs
import settings

settings.set_env('superhero_bi')
# settings.set_env('superhero_qiku')
name ='g497'
# name ='q74'
date1 = '20160401'
date2 = '20160410'

vip0_sql = "select ds,count(distinct uid) vip0 from raw_info where vip_level =0 and reverse(substring(reverse(uid), 8)) = '{name}' and ds >='{date1}' and ds <= '{date2}' group by ds order by ds".format(**{
    'name' : name,
    'date1': date1,
    'date2': date2,
    })

vip_sql = "select ds,count(distinct uid) vip from raw_info where vip_level >0 and reverse(substring(reverse(uid), 8)) = '{name}' and ds >='{date1}' and ds <= '{date2}' group by ds order by ds".format(**{
    'name' : name,
    'date1': date1,
    'date2': date2,
    })

reg_sql = "select ds,count(distinct uid) reg_num from raw_reg where ds >='{date1}'  and reverse(substring(reverse(uid), 8)) = '{name}' and ds <= '{date2}' group by ds order by ds".format(**{
    'name' : name,
    'date1': date1,
    'date2': date2,
    })

pay_sql = "select ds,count(distinct user_id) pay_num,sum(order_money) sum_money from raw_paylog where  reverse(substring(reverse(user_id), 8)) = '{name}'  and  ds >='{date1}' and ds <= '{date2}' group by ds order by ds".format(**{
    'name' : name,
    'date1': date1,
    'date2': date2,
    })
pay6_sql = '''
select ds,count(distinct user_id) as pay6_num from
(
    select ds,user_id,sum(order_money) sum_money
    from raw_paylog
    where  reverse(substring(reverse(user_id), 8)) = '{name}'
    and ds >='{date1}' and ds <= '{date2}' group by ds,user_id
)a
where sum_money=6
group by ds
order by ds
'''.format(**{
    'name' : name,
    'date1': date1,
    'date2': date2,
    })

reg_df,vip0_df,vip_df,pay_df,pay6_df = hqls_to_dfs([reg_sql,vip0_sql,vip_sql,pay_sql,pay6_sql])

result =  (reg_df
                .merge(vip0_df,on=['ds'],how='outer')
                .merge(vip_df,on=['ds'],how='outer')
                .merge(pay_df,on=['ds'],how='outer')
                .merge(pay6_df,on=['ds'],how='outer')
            )
columns = ['ds','vip','vip0','pay_num','sum_money','pay6_num','reg_num']
result = result[columns]
result.to_excel('/Users/kaiqigu/Downloads/Excel/g497.xlsx')
