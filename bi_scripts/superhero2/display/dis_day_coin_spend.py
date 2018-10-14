#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 每日钻石消费
create_date : 2016.07.18
'''
import settings_dev
from utils import hql_to_df, update_mysql, date_range


def dis_day_coin_spend(date):
    table = 'dis_day_coin_spend'
    sql = '''
    
    SELECT '{date}' AS ds,
           server,
           vip,
           sum(dau) AS dau,
           sum(free_coin + nvl(charge_coin,0)) AS new_coin,
           sum(free_coin) AS free_get_coin,
           sum(charge_coin) AS pay_get_coin,
           sum(save_coin) AS coin_save,
           (0 - sum(diamond_free_diff + diamond_charge_diff )) AS coin_spend
    FROM
      ( SELECT user_id,
               count(DISTINCT user_id) AS dau
        FROM parse_info
        WHERE ds = '{date}'
	    and  user_id not in (
			 select distinct user_id from raw_paylog where ds>='20180101'  and ds <='{date}' and platform='admin_test'
	         )
         GROUP BY user_id ) t5 
	LEFT OUTER JOIN
	
      ( SELECT user_id,
               sum(diamond_free_diff) as free_coin
        FROM parse_action_log
        WHERE ds = '{date}'
        AND (diamond_charge_diff  > 0 OR diamond_free_diff > 0)
        GROUP BY user_id ) t1  ON t5.user_id = t1.user_id
    LEFT OUTER JOIN
      ( SELECT user_id,
               sum(nvl(diamond_free_diff,0)) as diamond_free_diff,
               sum(nvl(diamond_charge_diff ,0)) as diamond_charge_diff
       FROM parse_action_log
       WHERE ds = '{date}'
         AND (diamond_charge_diff  < 0
              OR diamond_free_diff < 0)
       GROUP BY user_id ) t2 ON t5.user_id = t2.user_id
    LEFT OUTER JOIN
      ( SELECT user_id,
               sum(nvl(order_diamond,0)) + sum(nvl(gift_diamond,0)) AS charge_coin
       FROM raw_paylog
       WHERE ds = '{date}'
         AND platform != 'admin_test' and order_id not like '%testktwwn%'
       GROUP BY user_id ) t3 ON t5.user_id = t3.user_id
    LEFT OUTER JOIN
      ( SELECT user_id,
               reverse(substr(reverse(user_id), 8)) AS server,
               sum(nvl(diamond_free,0) + nvl(diamond_charge,0)) AS save_coin,
               vip
       FROM parse_info
       WHERE ds = '{date}'
       GROUP BY user_id,vip,reverse(substr(reverse(user_id), 8))) t4 ON t5.user_id = t4.user_id
    
   GROUP BY server,vip 
    
   '''.format(**{
        'date': date
    })
    print sql
    df = hql_to_df(sql)
    result_df = df.fillna(0)
    print result_df
    print date, table
    # 更新MySQL
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)

# 执行
if __name__ == '__main__':
    for platform in ['superhero2', ]:
    #for platform in ['superhero2_tw','superhero2']:
        settings_dev.set_env(platform)
        for date in date_range('20180712', '20180715'):
            dis_day_coin_spend(date)
        # dis_day_coin_spend('20170110')