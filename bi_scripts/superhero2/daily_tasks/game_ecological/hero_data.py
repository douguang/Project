#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: hero_data.py 
@time: 17/11/16 上午2:24 
"""

import pandas as pd
import settings_dev
from utils import hql_to_df,ds_add, date_range

def data_reduce():
    settings_dev.set_env('superhero2')
    info_sql = '''
        select t8.hero_id,t8.account_num,t8.pingjun_combat,t7.account5_num,t7.pingjun5_combat from (
    
        select t3.hero_id,count(distinct t3.account)as account_num,avg(t3.combat) as pingjun_combat from (
        select t1.user_id,t1.combat,t1.hero_id,t2.account from(
        select user_id,combat, hero_id from parse_hero where ds='20171113' and user_id in (select user_id from parse_info where ds='20171113' and regexp_replace(to_date(reg_time),'-','') = '20171113')
        )t1 left outer join(
          select account,user_id from parse_info where ds='20171113' group by account,user_id
          )t2 on t1.user_id=t2.user_id
          group by t1.user_id,t1.combat,t1.hero_id,t2.account
        )t3
        group by t3.hero_id
    
        )t8 left outer join(
    
        select t6.hero_id,count(distinct t6.account)as account5_num,avg(t6.combat) as pingjun5_combat from (
    
        select t4.user_id,row_number() over(partition BY t4.user_id
                                     ORDER BY t4.combat DESC) AS rank,t4.combat,t4.hero_id,t5.account from(
        select user_id,combat, hero_id from parse_hero where ds='20171113' and user_id in (select user_id from parse_info where ds='20171113' and regexp_replace(to_date(reg_time),'-','') = '20171113')
        )t4 left outer join(
          select account,user_id from parse_info where ds='20171113' group by account,user_id
          )t5 on t4.user_id=t5.user_id
          group by t4.user_id,t4.combat,t4.hero_id,t5.account
    
        )t6
        where t6.rank <=5
        group by t6.hero_id
    
        )t7 on t8.hero_id=t7.hero_id
        group by t8.hero_id,t8.account_num,t8.pingjun_combat,t7.account5_num,t7.pingjun5_combat
    '''
    print info_sql
    reg_df = hql_to_df(info_sql)
    print reg_df.head()



    # result_df = vip_df.merge(reg_df, on=['ds', ],how='left')

    return reg_df

if __name__ == '__main__':
    res_df = data_reduce()

    res_df.to_excel('/Users/kaiqigu/Documents/Superhero/超级英雄2-策划-商城数据_20171115.xlsx', index=False)
    print 'end'

