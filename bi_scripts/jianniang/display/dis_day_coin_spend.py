#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 每日钻石消费
create_date : 2016.07.18
illustration: Dong Junshuang 2017.05.26日更新，为提升页面打开速度去掉了server
'''
import settings_dev
from utils import hql_to_df
from utils import date_range
from utils import update_mysql


def dis_day_coin_spend(date):
    table = 'dis_day_coin_spend'
    # 钻石新增和消费
    new_coin_sql = '''
        select '{date}' as ds, sum(coin_diff) as new_coin, reverse(substr(reverse(user_id), 12)) as server from parse_actionlog where ds='{date}' and coin_diff > 0 and a_typ != 'player_reconnect_req' group by server, ds
    '''.format(**{'date': date})
    print new_coin_sql
    new_coin_df = hql_to_df(new_coin_sql)

    coin_spend_sql = '''
        select '{date}' as ds, sum(coin_diff) as coin_spend, reverse(substr(reverse(user_id), 12)) as server from parse_actionlog where ds='{date}' and coin_diff < 0 group by server, ds
    '''.format(**{'date': date})
    print coin_spend_sql
    coin_spend_df = hql_to_df(coin_spend_sql)

    # 充值钻石
    pay_get_coin_sql = '''
        select '{date}' as ds, sum(order_coin) as pay_get_coin, reverse(substr(reverse(user_id), 12)) as server from raw_paylog where ds='{date}' group by server, ds
    '''.format(**{'date': date})
    print pay_get_coin_sql
    pay_get_coin_df = hql_to_df(pay_get_coin_sql)

    # 钻石存量和DAU
    coin_save_sql = '''
        select '{date}' as ds, sum(coin) as coin_save, count(distinct user_id) as dau, reverse(substr(reverse(user_id), 12)) as server from raw_info where ds='{date}' group by server, ds
    '''.format(date=date)
    print coin_save_sql
    coin_save_df = hql_to_df(coin_save_sql)

    # 组合结果
    result_df = coin_save_df.merge(new_coin_df, on=['ds', 'server'], how='left')\
        .merge(coin_spend_df, on=['ds', 'server'], how='left')\
        .merge(pay_get_coin_df, on=['ds', 'server'], how='left').fillna(0)
    result_df['free_get_coin'] = result_df['new_coin'] - result_df['pay_get_coin']
    result_df['coin_spend'] = 0 - result_df['coin_spend']
    result_df['vip_level'] = 0                                                                              # 测试期暂时没有VIP
    result_df = result_df[result_df['server'] != '']
    columns = ['ds', 'server', 'vip_level', 'dau', 'new_coin', 'pay_get_coin', 'free_get_coin', 'coin_spend', 'coin_save']
    result_df = result_df[columns]
    print result_df.head(10)

    print date, table
    # 更新MySQL
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)


if __name__ == '__main__':
    for platform in ['jianniang_tw']:
        settings_dev.set_env(platform)
        for date in date_range('20170706', '20170710'):
            dis_day_coin_spend(date)
        # dis_day_coin_spend('20170110')