#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 20170425日    o武娘国服 近一个月新增玩家成长礼包购买情况,各等级礼包的符合等级人数、购买人数
Database    : dancer_pub
'''
import settings_dev
from utils import hql_to_df, update_mysql, ds_add, date_range
from pandas import DataFrame
import pandas as pd

def tmp_20170425_level_gift(start_date, date):

    # 购买人数
    level_gift_sql = '''
        select user_id, server, a_typ, a_tar, ds from parse_actionlog where ds>='{start_date}' and ds<='{date}' and a_typ='user.get_level_gift' and a_tar like '%lv%'
        and user_id in (select distinct user_id from mid_info_all where ds='{date}' and regexp_replace(to_date(reg_time), '-', '')>='{start_date}')
    '''.format(start_date=start_date, date=date)
    print level_gift_sql
    level_gift_df = hql_to_df(level_gift_sql)
    print level_gift_df.head(10)

    # server_df = pd.read_excel(r'E:\Data\output\dancer\server_date.xlsx')
    # level_gift_df = level_gift_df[level_gift_df['server'].isin(server_df['server'])]

    user_id_list, a_typ_list, lv_list, server_list, ds_list = [], [], [], [], []
    for _, row in level_gift_df.iterrows():
        tar = row['a_tar']
        tar = eval(tar)
        if tar.has_key('lv'):
            user_id_list.append(row['user_id'])
            server_list.append(row['server'])
            a_typ_list.append(row['a_typ'])
            lv_list.append(tar['lv'])
            ds_list.append(row['ds'])

    data_df = DataFrame({'user_id': user_id_list, 'server': server_list, 'a_typ': a_typ_list, 'lv': lv_list, 'ds': ds_list })
    # result_df = result_df.merge(server_df, on='server', how='left')
    gift_df = data_df.groupby(['lv', 'ds', 'a_typ']).agg({'user_id': lambda g: g.count()}).reset_index()
    print gift_df.head(10)
    gift_df.to_excel(r'E:\Data\output\dancer\level_gift.xlsx')

    # 资格人数
    level_sql = '''
        select count(user_id) as num, level as lv, ds from parse_info where  ds>='{start_date}' and ds<='{date}' and regexp_replace(to_date(reg_time), '-', '')>='{start_date}' group by ds, level
    '''.format(start_date=start_date, date=date)
    print level_sql
    level_df = hql_to_df(level_sql)
    # print level_df.head(10)
    level_df['lv'] = level_df['lv'].astype("int")
    print level_df.head(10)
    level_df.to_excel(r'E:\Data\output\dancer\level.xlsx')

    # result_df = gift_df.merge(level_df, on=['lv', 'ds'], how='left').reset_index()
    # return result_df


if __name__ == '__main__':
    settings_dev.set_env('dancer_pub')
    result = tmp_20170425_level_gift('20170325', '20170424')
    # result.to_excel(r'E:\Data\output\dancer\level_gift.xlsx')
