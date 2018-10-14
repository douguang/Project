#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 历史充值排行榜(全服)
Database    : dancer_ks
'''
import settings_dev
from utils import hql_to_df, update_mysql, date_range
def dis_history_pay_rank(date):
    server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    sql = '''
    SELECT '{date}' AS ds,
           server,
           rank,
           t1.uid,
           pay,
           pay_total,
           last_pay_time,
           vip,
           level,
           reg_time
    FROM
      (SELECT uid,
              sum(order_rmb) AS pay_total,
              max(to_date(order_time)) AS last_pay_time,
              reverse(substr(reverse(uid), 8)) AS server,
              Row_Number() OVER (ORDER BY sum(order_rmb) DESC) rank
       FROM raw_paylog
       WHERE ds >= '{server_start_date}'
         AND ds <= '{date}'
         AND platform != 'admin_test' AND order_id not like '%test%'
       GROUP BY uid) t1
    LEFT OUTER JOIN
      (SELECT uid,
              vip,
              level,
              reg_time
       FROM mid_info_all
       WHERE ds = '{date}') t2 ON t1.uid = t2.uid
    LEFT OUTER JOIN
      (SELECT uid,
              sum(order_rmb) AS pay
       FROM raw_paylog
       WHERE ds = '{date}'
         AND platform != 'admin_test' and order_id not like '%test%'
       GROUP BY uid) t3 ON t1.uid = t3.uid
    WHERE rank <= 100
    ORDER BY rank
    '''.format(date=date,server_start_date=server_start_date)
    df = hql_to_df(sql)
    df = df.fillna(0)
    # print df
    #更新MySQL
    table = 'dis_history_pay_rank'
    print date,table
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, df, del_sql)
#执行
if __name__ == '__main__':
    for platform in ['crime_empire_pub']:
        settings_dev.set_env(platform)
        # date = '20170114'
        # result = dis_ip_country_ltv(date)
        # result.to_excel('/home/kaiqigu/Documents/LTV.xlsx')
        for date in date_range('20171101', '20171105'):
            dis_history_pay_rank(date)
