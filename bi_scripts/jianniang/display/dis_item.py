#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 卡牌进阶、升星。
'''
import settings_dev
import pandas as pd
from utils import ds_add, hql_to_df, update_mysql, get_config, date_range
from jianniang.cfg import item_name_dict


def dis_item(date):

    item_sql = '''
        select t1.user_id, t1.server, t2.vip, t1.item_id, t1.amount from
        (select reverse(substr(reverse(user_id), 12)) as server, user_id, item_id, amount from raw_item where ds='{date}') t1
        left join
        (select user_id, vip from raw_info where ds='{date}') t2
        on t1.user_id=t2.user_id
    '''.format(date=date)
    item_df = hql_to_df(item_sql)

    # info_config = get_config('hero_base')
    item_df['amount'] = item_df['amount'].astype('int')
    item_df['ds'] = date
    columns = ['ds', 'user_id', 'server', 'vip', 'item_id', 'amount']
    item_df = item_df[columns]

    # 卡牌升星
    item_have_num_df = item_df.groupby(['ds', 'server', 'vip', 'item_id']).user_id.count().reset_index().rename(
        columns={'user_id': 'have_user_num'})
    item_all_num_df = item_df.groupby(['ds', 'server', 'vip', 'item_id']).amount.sum().reset_index().rename(
        columns={'amount': 'all_num'})
    item_result_df = item_all_num_df.merge(item_have_num_df, on=['ds', 'server', 'vip', 'item_id'], how='left')
    item_result_df['item_name'] = item_result_df['item_id'].map(item_name_dict)
    columns = ['ds', 'server', 'vip', 'item_id', 'item_name', 'have_user_num', 'all_num']
    item_result_df = item_result_df[columns]
    print item_result_df

    table = 'dis_item_amount'
    print date, table
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, item_result_df, del_sql)

if __name__ == '__main__':
    for platform in ['jianniang_tw']:
        settings_dev.set_env(platform)
        for date in date_range('20170706', '20170710'):
            # print date
            dis_item(date)