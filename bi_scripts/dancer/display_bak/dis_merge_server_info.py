#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 合服所需数据
create_date : 2016.08.12
'''
import settings_dev
from utils import update_mysql, hql_to_df, ds_add, date_range


def dis_merge_server_info(date):
    sql_1 = '''
    SELECT '{date}' AS ds,
           t1.server,
           combat,
           d7_pay_rmb,
           cast((dau / 7) AS bigint) AS d7_avg_dau,
           d7_pay_user_num
    FROM
      (SELECT reverse(substr(reverse(user_id),8)) AS server,
              max(combat) AS combat,
              count(user_id) AS dau
       FROM parse_info
       WHERE ds >= '{date_ago}'
         AND ds <= '{date}'
       GROUP BY reverse(substr(reverse(user_id),8))) t1
    LEFT OUTER JOIN
      (SELECT reverse(substr(reverse(user_id),8)) AS server,
              sum(order_money) AS d7_pay_rmb,
              count(DISTINCT user_id) AS d7_pay_user_num
       FROM raw_paylog
       WHERE ds >= '{date_ago}'
         AND ds <= '{date}' and platform_2<>'admin_test' AND order_id not like '%testktwwn%'
       GROUP BY reverse(substr(reverse(user_id),8))) t2 ON t1.server = t2.server
    '''.format(date=date, date_ago=ds_add(date, -6))
    sql_2 = '''
    SELECT server,
           cast((sum(t1.combat) / 10) as bigint) AS ten_avg_combat
    FROM
      ( SELECT reverse(substr(reverse(user_id), 8)) AS server,
               combat,
               rank() over(PARTITION BY reverse(substr(reverse(user_id), 8))
                           ORDER BY combat DESC) rk
       FROM parse_info
       WHERE ds = '{date}' ) t1
    WHERE t1.rk <= 10
    GROUP BY server'''.format(date=date)
    df1 = hql_to_df(sql_1)
    df2 = hql_to_df(sql_2)
    df = df1.merge(df2, on='server', how='left')
    df['ds'] = date
    columns = [
        'ds',
        'server',
        'd7_avg_dau',
        'd7_pay_user_num',
        'd7_pay_rmb',
        'combat',
        'ten_avg_combat']
    df = df[columns]
    df = df.fillna(0)
    print df

    # 更新MySQL
    table = 'dis_merge_server_info'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, df, del_sql)
    print date, table
# 执行
if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    for date in date_range('20161018', '20161019'):
        dis_merge_server_info(date)
