#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-3-20 下午2:59
@Author  : Andy 
@File    : dis_liucun_combat_distribution.py
@Software: PyCharm
Description :  次日留存玩家的战力分布
'''


from utils import hql_to_df, ds_add, date_range
import settings_dev
import pandas as pd


def dis_liucun_combat_distribution(date):
    print date
    ciliucun_sql = '''
        select t1.ds,
          sum(case when t2.score  <= 1000 then 1 else 0 end) as score_0_1000,
          sum(case when t2.score  >1000 and t2.score<= 2000 then 1 else 0 end) as score_1000_2000,
          sum(case when t2.score  >2000 and t2.score<= 3000 then 1 else 0 end) as score_2000_3000,
          sum(case when t2.score  >3000 and t2.score<= 5000 then 1 else 0 end) as score_3000_5000,
          sum(case when t2.score  >5000 and t2.score<= 8000 then 1 else 0 end) as score_5000_8000,
          sum(case when t2.score  >8000 and t2.score<= 12000 then 1 else 0 end) as score_8000_12000,
          sum(case when t2.score  >12000 and t2.score<= 15000 then 1 else 0 end) as score_12000_15000,
          sum(case when t2.score  >15000 and t2.score<= 20000 then 1 else 0 end) as score_15000_20000,
          sum(case when t2.score  >20000 and t2.score<= 30000 then 1 else 0 end) as score_20000_30000,
          sum(case when t2.score  >30000 and t2.score<= 40000 then 1 else 0 end) as score_30000_40000,
          sum(case when t2.score  >40000 and t2.score<= 50000 then 1 else 0 end) as score_40000_50000,
          sum(case when t2.score  >50000 and t2.score<= 60000 then 1 else 0 end) as score_50000_60000,
          sum(case when t2.score  >60000 and t2.score<= 80000 then 1 else 0 end) as score_60000_80000,
          sum(case when t2.score  >80000 and t2.score<= 100000 then 1 else 0 end) as score_80000_100000,
          sum(case when t2.score  >100000 then 1 else 0 end) as score_100000
        from (
          select ds,user_id
          from raw_info
          where ds='{date}'
          and regexp_replace(substring(reg_time,1,10),'-','') = '{date}'
          and user_id in
            (select user_id
            from raw_info
            where ds='{tormo_date}'
            group by user_id)
          and user_id !='' group by ds,user_id
        )t1
        left outer join(
          select ds,user_id,score from raw_sword_rank where ds='{date}' group by  ds,user_id,score
        )t2 on t1.user_id=t2.user_id and t1.ds=t2.ds
        group by t1.ds
    '''.format(date=date, tormo_date=ds_add(date, 1))
    print ciliucun_sql
    ciliucun_df = hql_to_df(ciliucun_sql)
    print ciliucun_df.head(3)

    ciliucun_df.to_excel('/home/kaiqigu/桌面/剑娘-次日留存玩家的战力分布.xlsx', index=False)
if __name__ == '__main__':
    platform = 'jianniang_test'
    date = '20170318'
    settings_dev.set_env(platform)
    dis_liucun_combat_distribution(date)
