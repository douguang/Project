#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 通天钱庄
Time        : 2017.06.19
illustration: 投资一次就算参与了，充值金额：当日参与通天钱庄的人的充值总额
通天钱庄投资接口: gringotts.investing
VIP DAU 参与人数 参与投资次数 充值金额
'''
import settings_dev
# from utils import hql_to_df

if __name__ == '__main__':
    settings_dev.set_env('dancer_pub')
    sql = '''
    SELECT a.user_id,
           a.vip_level,
           a.attend_num,
           nvl(b.sum_money,0) AS total_money
    FROM
      (SELECT user_id,
              max(vip) AS vip_level,
              count(user_id) AS attend_num
       FROM parse_actionlog
       WHERE ds ='20170617'
         AND a_typ = 'gringotts.investing'
       GROUP BY user_id) a
    LEFT JOIN
      (SELECT user_id,
              sum(order_money) AS sum_money
       FROM raw_paylog
       WHERE ds ='20170617'
         AND platform_2 <> 'admin_test'
         AND order_id NOT LIKE '%test%'
       GROUP BY user_id) b ON a.user_id = b.user_id
    '''
    sql_dau = '''
    SELECT vip,
           count(user_id) AS uid_num
    FROM parse_info
    WHERE ds ='20170617'
    GROUP BY vip
    ORDER BY vip
    '''



