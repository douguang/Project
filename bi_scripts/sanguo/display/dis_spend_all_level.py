#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 钻石消费档次分布
Database    : sanguo_ks
Readline    : 日期	1-299
'''
import settings_dev
from utils import hql_to_df, update_mysql

def dis_spend_all_level(date):
    table = 'dis_spend_all_level'
    sql = '''
    select '{date}' as ds,
            sum(case when t1.spend >= 1 and  t1.spend <= 299 then 1 else 0 end) as coin1_299,
            sum(case when t1.spend >= 300 and  t1.spend <= 999 then 1 else 0 end) as coin300_999,
            sum(case when t1.spend >= 1000 and  t1.spend <= 1499 then 1 else 0 end) as coin1000_1499,
            sum(case when t1.spend >= 1500 and  t1.spend <= 2999 then 1 else 0 end) as coin1500_2999,
            sum(case when t1.spend >= 3000 and  t1.spend <= 4999 then 1 else 0 end) as coin3000_4999,
            sum(case when t1.spend >= 5000 and  t1.spend <= 7999 then 1 else 0 end) as coin5000_7999,
            sum(case when t1.spend >= 8000 and  t1.spend <= 9999 then 1 else 0 end) as coin8000_9999,
            sum(case when t1.spend >= 10000 and  t1.spend <= 14999 then 1 else 0 end) as coin10000_14999,
            sum(case when t1.spend >= 15000 and  t1.spend <= 19999 then 1 else 0 end) as coin15000_19999,
            sum(case when t1.spend >= 20000 and  t1.spend <= 29999 then 1 else 0 end) as coin20000_29999,
            sum(case when t1.spend >= 30000 and  t1.spend <= 40000 then 1 else 0 end) as coin30000_40000,
            sum(case when t1.spend >= 40001 then 1 else 0 end) as coin40001
    from
    (
        select user_id,
                sum(coin_num) as spend
        from raw_spendlog
        where ds = '{date}'
        group by user_id
    ) t1
    '''.format(**{'date': date})

    df = hql_to_df(sql)
    print df
    # 更新MySQL
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, df, del_sql)

# 执行
if __name__ == '__main__':
    settings_dev.set_env('sanguo_ks')
    dis_spend_all_level('20160426')
