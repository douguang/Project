#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 装备精炼。
'''
import settings_dev
import pandas as pd
from utils import ds_add, hql_to_df, update_mysql, get_config, date_range


def dis_equip_st_lv(date):

    equip_sql = '''
        select t1.user_id, t1.server, t2.vip, t1.equip_id, t1.st_lv, t1.owner from
        (select reverse(substr(reverse(user_id), 8)) as server, user_id, equip_id, st_lv, owner from parse_equip where ds='{date}') t1
        left join
        (select user_id, vip from parse_info where ds='{date}') t2
        on t1.user_id=t2.user_id
    '''.format(date=date)
    print equip_sql
    equip_df = hql_to_df(equip_sql)
    print equip_df.head(10)
    info_config = get_config('equip_basis')
    equip_df['st_lv'] = equip_df['st_lv'].astype('int')
    equip_df['ds'] = date
    columns = ['ds', 'user_id', 'server', 'vip', 'equip_id', 'st_lv', 'owner']
    equip_df = equip_df[columns]

    # 装备精炼
    equip_attend_num_df = equip_df.groupby(['ds', 'server', 'vip', 'equip_id']).user_id.count().reset_index().rename(columns={'user_id': 'have_user_num'})
    equip_have_num_df = equip_df[equip_df['owner'] != ''].groupby(['ds', 'server', 'vip', 'equip_id']).user_id.count().reset_index().rename(
        columns={'user_id': 'attend_num'})
    st_lv_dic = {num: 'st_lv_%d' % num for num in range(0, 11, 1)}
    equip_st_lv_df = pd.pivot_table(equip_df, values='user_id', index=['ds', 'server', 'vip', 'equip_id'], columns='st_lv', aggfunc='count', fill_value=0).reset_index().rename(columns=st_lv_dic)
    for i in ['st_lv_%d'%num for num in range(0, 11, 1)]:
        if i not in equip_st_lv_df.columns:
            equip_st_lv_df[i] = 0
    equip_st_lv_df = equip_st_lv_df.merge(equip_have_num_df, on=['ds', 'server', 'vip', 'equip_id'], how='left').merge(equip_attend_num_df, on=['ds', 'server', 'vip', 'equip_id'], how='left')
    equip_st_lv_df['equip_name'] = equip_st_lv_df['equip_id'].map(lambda x: info_config[str(x)]['name'])
    columns = ['ds', 'server', 'vip', 'equip_id', 'equip_name', 'have_user_num', 'attend_num'] + ['st_lv_%d'%d for d in range(0, 11, 1)]
    equip_st_lv_df = equip_st_lv_df[columns]
    # print equip_st_lv_df

    table = 'dis_equip_st_lv'
    print date, table
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, equip_st_lv_df, del_sql)


if __name__ == '__main__':
    for platform in ['superhero2_tw']:
        settings_dev.set_env(platform)
        for date in date_range('20170909', '20170912'):
            dis_equip_st_lv(date)