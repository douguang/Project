#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 分接口钻石消耗
Database    : sanguo_ks
Readline    : 日期    消耗接口英文    消耗接口中文  消耗量 参与人数    消耗次数    每人平均消耗  每次平均消耗
'''
import settings_dev
from utils import hql_to_df, update_mysql, date_range


def dis_spend_api(date):
    # 汇总的分接口钻石消耗
    sql = '''
    select '{date}' as ds,
           goods_type,
           sum(coin_num) as spend_all,
           count(distinct user_id) as user_num,
           count(order_id) as spend_num,
           cast(sum(coin_num) / count(distinct user_id) as int) as user_num_every,
           cast(sum(coin_num) / count(order_id) as int) as spend_num_every
    from raw_spendlog
    where ds = '{date}'
    group by goods_type;
    '''.format(**{'date': date})
    df = hql_to_df(sql)
    print df

    table_1 = 'dis_spend_api'
    del_sql = 'delete from {0} where ds="{1}"'.format(table_1, date)
    update_mysql(table_1, df, del_sql)

    # 分 vip 和 服务器 两个维度的分接口钻石消耗
    sql = '''
    select '{date}' as ds,
           server,
           vip,
           goods_type,
           sum(coin_num) as spend_all,
           count(distinct info.user_id) as user_num,
           count(order_id) as spend_num,
           cast(sum(coin_num) / count(distinct info.user_id) as int) as user_num_every,
           cast(sum(coin_num) / count(order_id) as int) as spend_num_every
    from (
        select order_id, coin_num, user_id, goods_type
        from raw_spendlog
        where ds = '{date}'
    ) spendlog
    join
    (
        select user_id, vip, reverse(substring(reverse(user_id), 8)) as server
        from mid_info_all
        where ds = '{date}'
    ) info on spendlog.user_id = info.user_id
    group by goods_type, vip, server
    '''.format(**{'date': date})
    df = hql_to_df(sql)
    print df

    table_2 = 'dis_spend_api_detail'
    del_sql = 'delete from {0} where ds="{1}"'.format(table_2, date)
    update_mysql(table_2, df, del_sql)


if __name__ == '__main__':
    for platform in ['metal_test',]:
        settings_dev.set_env(platform)
        for date in date_range('20160901', '20160905'):
            dis_spend_api(date)
