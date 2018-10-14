#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-11-25 下午7:02
@Author  : Andy 
@File    : activity_raiders.py
@Software: PyCharm
Description :天降神物
输出格式：中奖的uid以及中奖id(raiders_reward的id),并求该玩家的vip等级、当日充值及消费总额
'''

import settings_dev
from utils import hql_to_df

def tianjiangshenwu(date):
    shenwu_sql = '''
        select
          t1.user_id,t1.max_vip,t2.today_money,t3.today_sum,t1.a_typ,t1.a_tar
        from
          (
          select
            user_id,max(vip_level) as max_vip ,a_typ,a_tar
          from
            parse_actionlog
          where
            ds = '{date}' and a_typ like "%raiders%"
          group by
            user_id,a_typ,a_tar
          ) t1
        left outer join
          (
          select
            user_id,sum(order_money) as today_money
          from
            raw_paylog
          where
            ds = '{date}'
          group by
            user_id
          )t2
        on
          t1.user_id = t2.user_id
        left outer join
          (
          select
            user_id,sum(coin_num) as today_sum
          from
            raw_spendlog
          where
            ds = '{date}'
          group by
            user_id
        ) t3
        on
          t1.user_id = t3.user_id
        group by
          t1.user_id,t1.max_vip,t2.today_money,t3.today_sum,t1.a_typ,t1.a_tar
    '''.format(date=date)

    print shenwu_sql
    result = hql_to_df(shenwu_sql)
    return result

if __name__ == '__main__':
    settings_dev.set_env('sanguo_ks')
    date = '20161006'
    result = tianjiangshenwu(date)
    result.to_excel('/home/kaiqigu/Documents/ceshi4.xlsx')