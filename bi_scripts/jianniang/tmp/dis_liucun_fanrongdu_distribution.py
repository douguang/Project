#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-3-20 下午2:59
@Author  : Andy 
@File    : dis_liucun_fanrongdu_distribution.py
@Software: PyCharm
Description :  次日留存用户的繁荣度分布
'''

from utils import hql_to_df, ds_add, date_range
import settings_dev
import pandas as pd

def dis_liucun_combat_distribution(date):
    print date
    ciliucun_sql = '''
          select t1.ds,
          sum(case when t2.score  <= 20 then 1 else 0 end) as score_0_20,
          sum(case when t2.score  >20 and t2.score<= 30 then 1 else 0 end) as score_20_30,
          sum(case when t2.score  >30 and t2.score<= 40 then 1 else 0 end) as score_30_40,
          sum(case when t2.score  >40 and t2.score<= 50 then 1 else 0 end) as score_40_50,
          sum(case when t2.score  >50 and t2.score<= 60 then 1 else 0 end) as score_50_60,
          sum(case when t2.score  >60 and t2.score<= 70 then 1 else 0 end) as score_60_70,
          sum(case when t2.score  >70 and t2.score<= 80 then 1 else 0 end) as score_70_80,
          sum(case when t2.score  >80 and t2.score<= 90 then 1 else 0 end) as score_80_90,
          sum(case when t2.score  >90 and t2.score<= 100 then 1 else 0 end) as score_90_100,
           sum(case when t2.score  >100 and t2.score<= 200 then 1 else 0 end) as score_100_200,
          sum(case when t2.score  >200 and t2.score<= 300 then 1 else 0 end) as score_200_300,
          sum(case when t2.score  >300 and t2.score<= 400 then 1 else 0 end) as score_300_400,
          sum(case when t2.score  >400 and t2.score<= 500 then 1 else 0 end) as score_400_500,
          sum(case when t2.score  >500 and t2.score<= 600 then 1 else 0 end) as score_500_600,
          sum(case when t2.score  >600 and t2.score<= 700 then 1 else 0 end) as score_600_700,
          sum(case when t2.score  >700  then 1 else 0 end) as score_700
          from (
            select ds,user_id from raw_info where ds='{date}' and regexp_replace(substring(reg_time,1,10),'-','')  = '{date}'  and user_id in (select user_id from raw_info where ds='{tormo_date}' group by user_id)  group by ds,user_id
          )t1
          left outer join(
              select ds,user_id,score from raw_develop_rank where ds='{date}' group by  ds,user_id,score
          )t2 on t1.user_id=t2.user_id and t1.ds=t2.ds
          group by t1.ds
    '''.format(date=date,tormo_date=ds_add(date,1))
    print ciliucun_sql
    ciliucun_df = hql_to_df(ciliucun_sql)
    print ciliucun_df.head(3)

    ciliucun_df.to_excel('/home/kaiqigu/桌面/剑娘-次日留存玩家的战力分布.xlsx', index=False)
if __name__ == '__main__':
    platform = 'jianniang_test'
    date = '20170318'
    settings_dev.set_env(platform)
    dis_liucun_combat_distribution(date)


