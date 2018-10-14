#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : vip等级分布
'''
from utils import hql_to_df, hqls_to_dfs, ds_add, update_mysql, date_range
import settings_dev

def dis_vip_level_dst(date):
    new_sql = '''
    SELECT '{date}' AS ds,
           vip_level,
           count(DISTINCT t1.user_id) AS new_vip_user,
           sum(pay) AS new_vip_pay_coin
    FROM
      (SELECT user_id,
              vip AS vip_level
       FROM parse_info
       WHERE ds = '{date}'
         AND vip > 0
         AND user_id NOT IN
           (SELECT user_id
            FROM mid_info_all
            WHERE ds = '{date_ago}'
              AND vip > 0)) t1
    LEFT OUTER JOIN
      (SELECT user_id,
              sum(order_money) AS pay
       FROM raw_paylog
       WHERE ds = '{date}'
         AND platform_2 != 'admin_test' AND order_id not like '%testktwwn%'
       GROUP BY user_id) t2 ON t1.user_id = t2.user_id
    GROUP BY vip_level
     '''.format(date_ago=ds_add(date, -1), date=date)
    all_sql = '''
    SELECT '{date}' AS ds,
           vip_level,
           sum(pay) AS income,
           count(distinct t1.user_id) AS vip_user_total
    FROM
      (SELECT user_id,
              vip AS vip_level
       FROM mid_info_all
       WHERE ds = '{date}'
         AND vip > 0) t1
    LEFT OUTER JOIN
      (SELECT user_id,
              sum(order_money) AS pay
       FROM raw_paylog
       WHERE ds = '{date}'
         AND platform_2 != 'admin_test' AND order_id not like '%testktwwn%'
       GROUP BY user_id) t2 ON t1.user_id = t2.user_id
    GROUP BY vip_level
    '''.format(date=date)
    new_df, all_df = hqls_to_dfs([new_sql, all_sql])
    ori_df = all_df.merge(new_df,on=['ds','vip_level'],how='left').fillna(0)
    ori_df['old_vip_pay_coin'] = ori_df['income'] - ori_df['new_vip_pay_coin']
    ori_df['old_vip_user'] = ori_df['vip_user_total'] - ori_df['new_vip_user']
    ori_df['new_vip_login_rate'] = ori_df['new_vip_user'] / ori_df['vip_user_total']
    ori_df['new_vip_pay_rate'] = ori_df['new_vip_pay_coin'] / ori_df['income']
    columns = ['ds','vip_level','income','new_vip_user','old_vip_user','vip_user_total','new_vip_pay_coin','old_vip_pay_coin','new_vip_login_rate','new_vip_pay_rate']
    ori_df = ori_df[columns]
    result_df = ori_df.fillna(0)
    # print result_df
    # 更新MySQL表
    table = 'dis_vip_level_dst'
    print date,table
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)

if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    dis_vip_level_dst('20160802')
    # for date in date_range('20160722','20160802'):
    #     print date
    #     dis_vip_level_dst(date)
