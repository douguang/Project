#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 分接口钻石新增
Database    :
'''
import settings_dev
from utils import hql_to_df, update_mysql, date_range


def dis_coin_acqure_api(date):

    # 分 vip 和 服务器 两个维度的分接口钻石消耗
    sql = '''
        select
            '{date}' as ds,
            reverse(substring(reverse(user_id), 8)) as server,
            a_typ,
            sum(coin_diff) as acqure_coin,
            count(distinct user_id) as acqure_num
        from
            parse_actionlog
        where
            ds = '{date}' and coin_diff > 0 and reverse(substring(reverse(user_id), 8)) != '' and a_typ != 'player_reconnect_req'
        group by server, a_typ
    '''.format(**{'date': date})
    print sql
    df = hql_to_df(sql)
    df['acqure_every'] = df['acqure_coin'] * 1.0 / df['acqure_num']
    print df.head(10)

    table = 'dis_coin_acqure_api'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, df, del_sql)


if __name__ == '__main__':
    for platform in ['jianniang_tw']:
        settings_dev.set_env(platform)
        for date in date_range('20170706', '20170710'):
            dis_coin_acqure_api(date)
