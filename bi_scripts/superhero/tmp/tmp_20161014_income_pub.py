#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 找出列表中，对应服务器，钻石充值前10名的用户的累计充值金额
'''
import settings
from utils import hql_to_df,hqls_to_dfs

def temp_20161014_income_pub(server_list):

    income_sql = '''
    select * from (
      select
          uid,server,sum_money,row_number() over(partition by server order by sum_money desc ) as rn    /*加入rn序号，并取前10*/
      from
      (
        select
          uid,reverse(substring(reverse(uid), 8)) as server,sum(order_money) as sum_money
        from
          raw_paylog
        where
          ds >= '20160101' and ds <= '20161014' and reverse(substring(reverse(uid), 8)) in {server_list}
        group by
          uid,server
      )a
    )b
    where
      rn <= 10
        '''.format(server_list = tuple(server_list))                        #列表转元组，保证sql中传入的是（）
    print income_sql
    temp_20161014_income_pub_df = hql_to_df(income_sql)
    print temp_20161014_income_pub_df.head()
    return  temp_20161014_income_pub_df


if __name__ == '__main__':
    settings.set_env('superhero_bi')
    server_list = ['g32', 'g22', 'g25', 'g86', 'g89', 'g90', 'g94', 'g97', 'g99']
    result = temp_20161014_income_pub(server_list)
    result.to_excel('/home/kaiqigu/Documents/ceshi.xlsx')






