#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 三国钻石存量异常数据汇总
'''
import settings_dev
from utils import hql_to_df, update_mysql, ds_add


def dis_act_coinnum_info(date):
    table = 'dis_act_coinnum_info'

    act_coin_sql = '''
    select '{date}' as ds,
       coin_num,
       sum(case when vip=0 then 1 else 0 end) as vip0,
       sum(case when vip=1 then 1 else 0 end) as vip1,
       sum(case when vip=2 then 1 else 0 end) as vip2,
       sum(case when vip=3 then 1 else 0 end) as vip3,
       sum(case when vip=4 then 1 else 0 end) as vip4,
       sum(case when vip=5 then 1 else 0 end) as vip5,
       sum(case when vip=6 then 1 else 0 end) as vip6,
       sum(case when vip=7 then 1 else 0 end) as vip7,
       sum(case when vip=8 then 1 else 0 end) as vip8,
       sum(case when vip=9 then 1 else 0 end) as vip9,
       sum(case when vip=10 then 1 else 0 end) as vip10,
       sum(case when vip=11 then 1 else 0 end) as vip11,
       sum(case when vip=12 then 1 else 0 end) as vip12,
       sum(case when vip=13 then 1 else 0 end) as vip13,
       sum(case when vip=14 then 1 else 0 end) as vip14,
       sum(case when vip=15 then 1 else 0 end) as vip15
    from
    (
        select user_id,
               vip,
               coin_num
        from
        (
            select  user_id,
                    vip,
                    case when coin >=50000 then 's50000+'
                         when coin >=5000 and coin <=10000 then 's5000_10000'
                         when coin >=10001 and coin <=15000 then 's10001_15000'
                         when coin >=15001 and coin <=20000 then 's15001_20000'
                         when coin >=20001 and coin <=30000 then 's20001_30000'
                         when coin >=30001 and coin <=50000 then 's30001_50000'
                         else 'None' end as coin_num
            from mid_info_all
            where ds = '{date}'
        ) a
        where coin_num !='None'
    ) t1
    left semi join
    (
        select  user_id
        from    raw_activeuser
        where   ds <= '{date}' and ds >= '{date_in_3days}'
    ) t2 on t1.user_id = t2.user_id
    group by coin_num
    '''.format(**{
        'date': date,
        'date_in_3days': ds_add(date, -2),
    })

    print act_coin_sql
    act_coin_df = hql_to_df(act_coin_sql)

    # 更新MySQL表
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, act_coin_df, del_sql)

    return act_coin_df


if __name__ == '__main__':
    settings_dev.set_env('metal_test')
    result = dis_act_coinnum_info('20160426')
