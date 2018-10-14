#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 分接口钻石消耗
Database    :
'''
import settings_dev
from utils import hql_to_df, update_mysql, date_range


def dis_coin_spend_api(date):

    # 分 vip 和 服务器 两个维度的分接口钻石消耗
    sql = '''
        select
            '{date}' as ds,
            reverse(substring(reverse(user_id), 8)) as server,
            a_typ,
            sum(coin_diff) as spend_coin,
            count(distinct user_id) as spend_num,
            count(user_id) as spend_times
        from
            parse_actionlog
        where
            ds = '{date}' and coin_diff < 0 and reverse(substring(reverse(user_id), 8)) != ''
        group by server, a_typ
    '''.format(**{'date': date})
    print sql
    df = hql_to_df(sql)
    df['spend_coin'] = 0 - df['spend_coin']
    df['spend_every'] = df['spend_coin'] * 1.0 / df['spend_num']
    df['spend_each_time'] = df['spend_coin'] * 1.0 / df['spend_times']
    print df.head(10)

    table = 'dis_coin_spend_api'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, df, del_sql)


if __name__ == '__main__':
    for platform in ['jianniang_test']:
        settings_dev.set_env(platform)
        for date in date_range('20170524', '20170605'):
            dis_coin_spend_api(date)
