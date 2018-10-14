#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 合服数据需求
'''
import settings
from utils import hql_to_df, update_mysql, ds_add
from pandas import Series,DataFrame
from utils import get_config
import pandas as pd

def avg_7dau_info(date):
    avg_7dau_sql = '''
    SELECT father_ser,
           sum(num)/7
    FROM
      (SELECT ds,
              c.father_ser,
              count(DISTINCT c.uid) num
       FROM
         ( SELECT a.ds,
                  a.server,
                  b.father_ser,
                  a.uid
          FROM
            ( SELECT ds,
                     reverse(substring(reverse(uid), 8)) AS server,
                     uid
             FROM raw_act
             WHERE ds >='{date_ago}'
               AND ds<='{date}' ) a
          LEFT OUTER JOIN
            ( SELECT son_ser,
                     father_ser,
                     ds
             FROM mid_ser_map
             WHERE ds >='{date_ago}'
               AND ds<='{date}' ) b ON a.ds = b.ds
          AND a.server = b.son_ser ) c
       GROUP BY ds,
                c.father_ser
       ORDER BY ds,
                c.father_ser )e
    GROUP BY father_ser
    ORDER BY father_ser
    '''.format(date=date,date_ago=ds_add(date,-6))
    avg_7dau_df = hql_to_df(avg_7dau_sql)
    return avg_7dau_df


def pay_num_info(date):
    pay_num_sql='''
    select father_ser,sum(pay_num) from (
    select ds,c.father_ser,count(distinct user_id) as pay_num from (
    select a.ds,a.server,b.father_ser,a.user_id  from (
    select ds,reverse(substring(reverse(user_id), 8)) as server,user_id from raw_paylog where ds >='{date_ago}' and ds<='{date}' and platform_2 <> 'admin_test'
    ) a
    left outer join
    (
      select son_ser,father_ser,ds from mid_ser_map where ds >='{date_ago}' and ds<='{date}'
    ) b
    on a.ds = b.ds and a.server = b.son_ser
    ) c group by ds,c.father_ser
    order by ds,c.father_ser
    )e group by father_ser
    order by father_ser
    '''.format(date=date,date_ago=ds_add(date,-6))
    pay_num_df = hql_to_df(pay_num_sql)
    return pay_num_df

def pay_money_info(date):
    pay_money_sql ='''
    select c.father_ser,sum(c.order_rmb) as pay_money from (
    select a.ds,a.server,b.father_ser,a.order_rmb  from (
    select ds,reverse(substring(reverse(user_id), 8)) as server,user_id,order_rmb from raw_paylog where platform_2 <> 'admin_test' and ds >='{date_ago}' and ds<='{date}'
    ) a
    left outer join
    (
      select son_ser,father_ser,ds from mid_ser_map where ds >='{date_ago}' and ds<='{date}'
    ) b
    on a.ds = b.ds and a.server = b.son_ser
    ) c group by c.father_ser
    order by c.father_ser
    '''.format(date=date,date_ago=ds_add(date,-6))
    pay_money_df = hql_to_df(pay_money_sql)
    return pay_money_df

def max_zhandouli_info(date):
    max_zhandouli_sql ='''
    select c.father_ser,max(c.zhandouli) as max_zhandouli from (
    select a.ds,a.server,b.father_ser,a.zhandouli  from (
      select ds,reverse(substring(reverse(uid), 8)) as server,zhandouli from raw_info where ds >='{date_ago}' and ds<='{date}'
    ) a
    left outer join
    (
      select son_ser,father_ser,ds from mid_ser_map where ds >='{date_ago}' and ds<='{date}'
    ) b
    on a.ds = b.ds and a.server = b.son_ser
    ) c group by c.father_ser
    order by c.father_ser
    '''.format(date=date,date_ago=ds_add(date,-6))
    max_zhandouli_df = hql_to_df(max_zhandouli_sql)
    return max_zhandouli_df

def avg_zhandouli_info(date):
    avg_zhandouli_sql ='''
    with mm as(
      select a.ds,a.server,b.father_ser,a.zhandouli  from (
      select ds,reverse(substring(reverse(uid), 8)) as server,zhandouli from raw_info where ds >='{date_ago}' and ds<='{date}'
    ) a
    left outer join
    (
      select son_ser,father_ser,ds from mid_ser_map where ds >='{date_ago}' and ds<='{date}'
    ) b
    on a.ds = b.ds and a.server = b.son_ser
    )
    ,nn AS (
     SELECT server,father_ser,zhandouli,ROW_NUMBER() OVER
     (PARTITION BY father_ser ORDER BY zhandouli DESC) as num  FROM mm
    )
    ,tt as (
      select server,father_ser,zhandouli,num from nn where num<=10
    )select father_ser,sum(zhandouli)/10 from tt group by father_ser order by father_ser
    '''.format(date=date,date_ago=ds_add(date,-6))
    avg_zhandouli_df = hql_to_df(avg_zhandouli_sql)
    return avg_zhandouli_df


if __name__ == '__main__':
    settings.set_env('superhero_vt')
    print 'please wait a minuate'
    date = '20160626'
    avg_7dau_df = avg_7dau_info(date)
    pay_num_df = pay_num_info(date)
    pay_money_df = pay_money_info(date)
    max_zhandouli_df = max_zhandouli_info(date)
    avg_zhandouli_df = avg_zhandouli_info(date)

    final_result =  avg_7dau_df.merge(pay_num_df,on=['father_ser'],how='outer')
    final_result =  final_result.merge(pay_money_df,on=['father_ser'],how='outer')
    final_result =  final_result.merge(max_zhandouli_df,on=['father_ser'],how='outer')
    final_result =  final_result.merge(avg_zhandouli_df,on=['father_ser'],how='outer')

    final_result.to_excel('/Users/kaiqigu/Downloads/Excel/hefu.xlsx')

