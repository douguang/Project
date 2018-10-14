#!/usr/env/python
# -*- coding:utf-8 -*-
'''
Author      : Hu Chunlong
Description : 各服务器每日充值金额排名Top30
'''
import settings_dev
from utils import hql_to_df, update_mysql

def dis_daily_pay_rank(date):
    table = 'dis_daily_pay_rank'
    daily_pay_rank_sql = '''
    select * from
    (
        select '{date}' as ds,
                reverse(substring(reverse(a.user_id), 8)) as server,
                row_number() over(order by a.pay desc) as rank,
                a.user_id,
                a.pay,
                b.pay_total,
                c.vip,
                c.level,
                c.reg_time
        from
        (
            select user_id,
                   sum(order_money) as pay
            from raw_paylog
            where ds = '{date}'
            group by user_id
        ) a
        left outer join
        (
            select user_id,
                   sum(order_money) as pay_total
            from raw_paylog
            group by user_id
        ) b on a.user_id = b.user_id
        left outer join
        (
            select user_id,
                   vip,
                   level,
                   reg_time
            from mid_info_all
            where ds = '{date}'
        ) c on a.user_id = c.user_id
    ) d
    where d.rank <= 30
    '''.format(**{'date': date})
    print daily_pay_rank_sql
    df = hql_to_df(daily_pay_rank_sql)

    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, df, del_sql)

if __name__ == '__main__':
    settings_dev.set_env('sanguo_ks')
    dis_daily_pay_rank('20160622')
