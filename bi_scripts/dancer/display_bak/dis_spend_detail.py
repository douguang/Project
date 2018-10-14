#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 分接口钻石消耗
create_date : 2016.07.18
'''
from utils import hql_to_df, ds_add, update_mysql
import settings_dev
import pandas as pd
from pandas import DataFrame
from dancer.cfg import zichong_uids

zichong_uids = str(tuple(zichong_uids))

def dis_spend_detail(date):
    spend_sql = '''
    SELECT '{date}' as ds,
           goods_type as api,
           server,
           sum(coin_num) as spend_num,
           vip_level
    FROM
      ( SELECT user_id,
               reverse(substr(reverse(user_id), 8)) AS server,
               goods_type,
               coin_num
       FROM raw_spendlog
       WHERE ds = '{date}' and user_id not in {zichong_uids}) t1
    LEFT OUTER JOIN
      ( SELECT user_id,
               vip as vip_level
       FROM mid_info_all
       WHERE ds = '{date}') t2 ON t1.user_id = t2.user_id
    GROUP BY goods_type, server, vip_level
    '''.format(date=date, zichong_uids=zichong_uids)
    result_df = hql_to_df(spend_sql)
    result_df = result_df.fillna(0)
    print result_df

    # 更新消费详情的MySQL表
    table = 'dis_spend_detail'
    print date,table
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)

if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    dis_spend_detail('20170110')
