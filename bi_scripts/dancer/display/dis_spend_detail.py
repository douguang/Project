#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 分接口钻石消耗
create_date : 2016.07.18
illustration: Dong Junshuang 2017.05.26日更新，为提升页面打开速度去掉了server
'''
# from utils import ds_add
from utils import date_range
from utils import hql_to_df
from utils import update_mysql
import settings_dev
from dancer.cfg import zichong_uids

zichong_uids = str(tuple(zichong_uids))


def dis_spend_detail(date):
    spend_sql = '''
    SELECT '{date}' AS ds,
           goods_type AS api,
           sum(coin_num) AS spend_num
    FROM raw_spendlog
    WHERE ds = '{date}'
      AND user_id NOT IN {zichong_uids}
    GROUP BY goods_type
    '''.format(date=date, zichong_uids=zichong_uids)
    result_df = hql_to_df(spend_sql)
    result_df = result_df.fillna(0)
    print result_df.head()

    # 更新消费详情的MySQL表
    table = 'dis_spend_detail'
    print date, table
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)


if __name__ == '__main__':
    for platform in ['dancer_tw']:
        settings_dev.set_env(platform)
        for date in date_range('20170401', '20170525'):
            dis_spend_detail(date)
