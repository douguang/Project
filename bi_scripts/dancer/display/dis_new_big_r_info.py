#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 新增鲸鱼用户分析
'''

import settings_dev
from utils import hql_to_df, update_mysql, ds_add


def dis_new_big_r_info(date):
    # 首次达到鲸鱼用户的判断标准
    pay_standard = 500
    # TODO：充值和消费做uid每日汇总，然后改写该sql
    new_big_r_info_sql = '''
    SELECT '{date}' AS ds,
           /*加入ds索引*/
           t1.user_id AS user_id,
           reg_time,
           LEVEL,
           vip,
           coalesce(spend_coin_sum, 0) AS spend_coin_sum,
           /*nan补0*/
           pay_rmb_sum,
           charge_coin_sum,
           first_pay_date,
           last_pay_date,
           d7_pay_date_num
    FROM
      ( SELECT user_id,
               sum(order_money) AS pay_rmb_sum,
               sum(gift_coin)+sum(order_coin) AS charge_coin_sum,
               /*充值钻石+赠送钻石*/
               min(ds) AS first_pay_date,
               max(ds) AS last_pay_date,
               sum(CASE WHEN ds < '{date}' THEN order_money ELSE 0 END) AS pay_rmb_sum_yestoday,
               /*截至昨天的充值金额 CASE WHEN THEN ELSE*/
               count(DISTINCT CASE WHEN ds >= '{date_before_seven}'
                     AND ds <= '{date}' THEN ds ELSE NULL END) AS d7_pay_date_num /*统计不同的充值日期作为充值天数*/
       FROM raw_paylog
       WHERE ds <= '{date}'
         AND platform_2<>'admin_test'
         AND order_id NOT LIKE '%test%'
       GROUP BY user_id HAVING pay_rmb_sum_yestoday < {pay_standard}
       AND pay_rmb_sum > {pay_standard} /*通过昨天与今天的累计充值情况以及设定的充值标准来确定鲸鱼*/ ) t1
    LEFT JOIN
      ( SELECT user_id,
               sum(coin_num) AS spend_coin_sum
       FROM raw_spendlog
       WHERE ds <= '{date}'
       GROUP BY user_id ) t2 ON t1.user_id = t2.user_id
    LEFT  JOIN
      ( SELECT user_id,
               reg_time,
               vip,
               LEVEL
       FROM mid_info_all
       WHERE ds = '{date}' ) t3 ON t1.user_id = t3.user_id
    '''.format(**{
        'date': date,
        'date_before_seven': ds_add(date, -7),
        'pay_standard': pay_standard
    })
    print new_big_r_info_sql
    new_big_r_info_df = hql_to_df(new_big_r_info_sql)

    # 更新MySQL表
    table = 'new_big_r_info'
    delete_sql = "delete from '{0}' where ds='{1}'".format(table, date)
    update_mysql(table, new_big_r_info_df, delete_sql)
    return new_big_r_info_df

# 测试文件，被调用时不会执行
if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    result = dis_new_big_r_info('20161010')
    print result.head(5)  #检查数据是否正确，默认查看五条
    result.to_excel('/home/kaiqigu/Documents/ceshi2.xlsx')
