#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 充值档次分布
Database    : sanguo_ks
Readline    : 日期    1 ... 4000+
'''
import settings_dev
from utils import hql_to_df, update_mysql, date_range

def dis_pay_all_level(date):
    # table = 'dis_pay_all_level'
    table = 'dis_pay_detail_level'
    sql = '''
    select '{date}' as ds,
            sum(case when t1.pay = 1 then 1 else 0 end) as rmb1,
            sum(case when t1.pay >= 2 and  t1.pay <= 5 then 1 else 0 end) as rmb2_5,
            sum(case when t1.pay = 6 then 1 else 0 end) as rmb6,
            sum(case when t1.pay >= 7 and  t1.pay <= 29 then 1 else 0 end) as rmb7_29,
            sum(case when t1.pay = 30 then 1 else 0 end) as rmb30,
            sum(case when t1.pay >= 31 and  t1.pay <= 49 then 1 else 0 end) as rmb31_49,
            sum(case when t1.pay = 50 then 1 else 0 end) as rmb50,
            sum(case when t1.pay >= 51 and  t1.pay <= 99 then 1 else 0 end) as rmb51_99,
            sum(case when t1.pay = 100 then 1 else 0 end) as rmb100,
            sum(case when t1.pay >= 101 and  t1.pay <= 299 then 1 else 0 end) as rmb101_299,
            sum(case when t1.pay = 300 then 1 else 0 end) as rmb300,
            sum(case when t1.pay >= 301 and  t1.pay <= 499 then 1 else 0 end) as rmb301_499,
            sum(case when t1.pay = 500 then 1 else 0 end) as rmb500,
            sum(case when t1.pay >= 501 and  t1.pay <= 799 then 1 else 0 end) as rmb501_799,
            sum(case when t1.pay = 800 then 1 else 0 end) as rmb800,
            sum(case when t1.pay >= 801 and  t1.pay <= 999 then 1 else 0 end) as rmb801_999,
            sum(case when t1.pay = 1000 then 1 else 0 end) as rmb1000,
            sum(case when t1.pay >= 1001 and  t1.pay <= 1499 then 1 else 0 end) as rmb1001_1499,
            sum(case when t1.pay = 1500 then 1 else 0 end) as rmb1500,
            sum(case when t1.pay >= 1501 and  t1.pay <= 1999 then 1 else 0 end) as rmb1501_1999,
            sum(case when t1.pay = 2000 then 1 else 0 end) as rmb2000,
            sum(case when t1.pay >= 2001 and  t1.pay <= 2999 then 1 else 0 end) as rmb2001_2999,
            sum(case when t1.pay = 3000 then 1 else 0 end) as rmb3000,
            sum(case when t1.pay >= 3001 and  t1.pay <= 3999 then 1 else 0 end) as rmb3001_3999,
            sum(case when t1.pay = 4000 then 1 else 0 end) as rmb4000,
            sum(case when t1.pay >= 4001 then 1 else 0 end) as rmb4001
    from
    (
        select user_id,
                sum(order_money) as pay
        from raw_paylog
        where ds = '{date}' and platform_2 != 'admin_test'
        group by user_id
    ) t1
    '''.format(**{'date': date})

    df = hql_to_df(sql)
    print df
    #更新MySQL
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, df, del_sql)

#执行
if __name__ == '__main__':
    for platform in ['metal_test', ]:
        settings_dev.set_env(platform)
        for date in date_range('20160728', '20160818'):
            dis_pay_all_level(date)
