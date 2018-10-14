#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-29 下午2:47
@Author  : Andy Zhang Yongchen
@File    : dis_activity_group_buy.py
@Software: PyCharm
Description :   团购活动
group_buy.group_active_buy	{u'count': u'1', u'devicename': u'GN9010L', u'item': u'2', u'channel_id': u'', u'identifier': u'868682020515793'}
'''

import pandas as pd
from pandas import DataFrame
from utils import get_active_conf, hql_to_df, ds_short, update_mysql, date_range
import settings

def dis_activity_group_buy(date):

    version, act_start_time, act_end_time = get_active_conf('group_version', date)
    print version, act_start_time, act_end_time
    if version != '':
        act_start_short = ds_short(act_start_time)
        act_end_short = ds_short(act_end_time)
        print version, act_start_time, act_end_time
        dis_activity_group_buy_bi(act_start_short,act_end_short,act_start_time,act_end_time)
    else:
        print '{0} 没有团购活动'.format(date)
        
def dis_activity_group_buy_bi(act_start_short,act_end_short,act_start_time,act_end_time):
    if settings.code == 'superhero_bi':
        for plat in ['superhero_pub', 'superhero_ios']:
            print plat
            dis_activity_group_buy_one(act_start_short, act_end_short, act_start_time, act_end_time, plat)
    else:
        dis_activity_group_buy_one(act_start_short, act_end_short, act_start_time, act_end_time)

def bi_sql(act_start_short, act_end_short, act_start_time, act_end_time, pp):

    sql = '''
        select uid,reverse(substr(reverse(uid), 8)) as server,args
        from raw_action_log
        WHERE ds >= '{act_start_short}'
        AND ds<='{act_end_short}'
        AND act_time >='{act_start_time}'
        AND act_time <= '{act_end_time}'
        and action='active.group_active_buy'
        and substr(uid,1,1) = '{pp}'
    '''.format(act_start_short=act_start_short,
               act_end_short=act_end_short,
               act_start_time=act_start_time,
               act_end_time=act_end_time,
               pp=pp)
    return sql

def dis_activity_group_buy_one(act_start_short,act_end_short,act_start_time,act_end_time,plat=None):

    sql = '''
        select uid,reverse(substr(reverse(uid), 8)) as server,args
        from raw_action_log
        WHERE ds >= '{act_start_short}'
        AND ds<='{act_end_short}'
        AND act_time >='{act_start_time}'
        AND act_time <= '{act_end_time}'
        and action='active.group_active_buy'
    '''.format(act_start_short=act_start_short,
               act_end_short=act_end_short,
               act_start_time=act_start_time,
               act_end_time=act_end_time)

    #根据plat更新sql
    if plat == 'superhero_pub':
        sql = bi_sql(act_start_short, act_end_short, act_start_time, act_end_time, 'g')
    if plat == 'superhero_ios':
        sql = bi_sql(act_start_short, act_end_short, act_start_time, act_end_time, 'a')

    print sql
    df = hql_to_df(sql)
    df = df.fillna(0)
    df['ds']=act_start_short
    print df.head(10)

    if df.__len__() != 0:
        ds_list, uid_list,server_list, item_id_list, count_list =  [], [], [], [],[]
        for i in range(len(df)):
            ds = df.iloc[i, 3]
            uid = df.iloc[i, 0]
            server = df.iloc[i, 1]
            tar = df.iloc[i, 2]
            tar = eval(tar)
            item_id = tar['shop_id'][0]
            count = int(tar['count'][0])
            ds_list.append(ds)
            uid_list.append(uid)
            server_list.append(server)
            item_id_list.append(item_id)
            count_list.append(count)

        data = DataFrame({'ds': ds_list,
                          'server': server_list,
                          'item_id': item_id_list,
                          'count': count_list,
                          'uid': uid_list})
        buy_num = data.groupby(['ds','server', 'item_id']).agg(
            {'uid': lambda g: g.nunique()}).reset_index()
        buy_num = buy_num.rename(columns={'uid': 'buy_user_num', })
        count_num = data.groupby(['ds','server', 'item_id']).agg(
            {'count': lambda g: g.sum()}).reset_index()
        count_num = count_num.rename(columns={'uid': 'buy_num', })

        result = DataFrame(buy_num).merge(
            count_num, on=['ds', 'server','item_id'], how='left')
        result_df = DataFrame(result).fillna(0)
        print result_df.head(10)


        result_df['ds'] = result_df['ds'].astype("str")
        result_df['server'] = result_df['server'].astype("str")
        result_df['item_id'] = result_df['item_id'].astype("int")
        result_df['buy_user_num'] = result_df['buy_user_num'].astype("int")
        result_df['count'] = result_df['count'].astype("int")
        print result_df.head(10)
        table = 'dis_activity_group_buy'
        del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
        update_mysql(table, result_df, del_sql, plat)

if __name__ == '__main__':
    for platform in ('superhero_bi', 'superhero_vt', 'superhero_qiku', 'superhero_tw', 'superhero_self_en'):
        settings.set_env(platform)
        for date in date_range('20161031', '20170118'):
            dis_activity_group_buy(date)

