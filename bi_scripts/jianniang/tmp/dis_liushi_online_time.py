#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-3-20 下午3:04
@Author  : Andy 
@File    : dis_liushi_online_time.py
@Software: PyCharm
Description :    次日流失用户的在线时长
        用户每日登陆时长，两次动作差在5min内算持续登陆，超过5min，算第二次登陆，重新计算登陆时长
'''

from utils import hql_to_df, ds_add, date_range
import settings_dev
import pandas as pd

def dis_liushi_stay_on_checkpoint_last(date):
    print date
    # 流失用户
    ciliushi_sql = '''
        select ds,user_id from raw_info where ds='{date}' and regexp_replace(substring(reg_time,1,10),'-','')  = '{date}'  and user_id not in (select user_id from raw_info where ds='{tomor_date}' group by user_id)  group by ds,user_id
    '''.format(date=date, tomor_date=ds_add(date, 1))
    print ciliushi_sql
    ciliushi_df = hql_to_df(ciliushi_sql).fillna(0)

    # 用户当日的登录情况
    ds_act_sql = '''
            select ds,user_id,unix_timestamp(log_t,'yyyy-MM-dd HH:mm:ss') as ts from parse_actionlog where ds='{date}'  group by ds,user_id,ts order by ds,user_id,ts desc
        '''.format(date=date)
    print ds_act_sql
    ds_act_df = hql_to_df(ds_act_sql).fillna(0)

    result_df = ciliushi_df.merge(ds_act_df, on=['ds', 'user_id', ],).fillna(0)
    result_df['ts'] = result_df['ts'].astype(int)
    # 迭代计算
    result_df['ts_differ'] = 0
    for i in range(len(result_df)):
        if i == len(result_df)-1:
            result_df.iloc[i, 3]=0
        else:
            if result_df.iloc[i, 0]== result_df.iloc[i+1, 0] and result_df.iloc[i, 1]== result_df.iloc[i+1, 1]:
                result_df.iloc[i, 3]=result_df.iloc[i, 2] - result_df.iloc[i+1, 2]

    result_df[result_df['ts_differ'] > 300] = 0
    # 计算在线时长
    result_df = result_df[['ds','user_id','ts_differ']]
    result_df = pd.DataFrame(result_df).groupby(['ds', 'user_id']).agg(
        {'ts_differ': lambda g: g.sum()}).reset_index()
    result_df = result_df.rename(
        columns={'ts_differ': 'online_time', })

    # 后台统计时只是需要将每个用户的在线时长存入即可  无需分组
    result_list = []

    a_df = result_df[result_df['online_time'] < 300]
    a_df.online_time='5min'
    result_list.append(a_df)

    b_df = result_df[(result_df.online_time >= 300) & (result_df.online_time< 600)]
    b_df.online_time = '5-10min'
    result_list.append(b_df)

    c_df = result_df[(result_df.online_time >= 600) & (result_df.online_time< 900)]
    c_df.online_time = '10-15min'
    result_list.append(c_df)

    d_df = result_df[(result_df.online_time >= 900) & (result_df.online_time< 1200)]
    d_df.online_time = '15-20min'
    result_list.append(d_df)

    e_df = result_df[(result_df.online_time >= 1200) & (result_df.online_time< 1500)]
    e_df.online_time = '20-25min'
    result_list.append(e_df)

    f_df = result_df[(result_df.online_time >= 1500) & (result_df.online_time< 1800)]
    f_df.online_time = '25-30min'
    result_list.append(f_df)

    g_df = result_df[(result_df.online_time >= 1800) & (result_df.online_time< 2100)]
    g_df.online_time = '30-35min'
    result_list.append(g_df)

    h_df = result_df[(result_df.online_time >= 2100) & (result_df.online_time< 2400)]
    h_df.online_time = '35-40min'
    result_list.append(h_df)

    i_df = result_df[(result_df.online_time >= 2400) & (result_df.online_time< 2700)]
    i_df.online_time = '40-45min'
    result_list.append(i_df)

    j_df = result_df[(result_df.online_time >= 2700) & (result_df.online_time< 3000)]
    j_df.online_time = '45-50min'
    result_list.append(j_df)

    k_df = result_df[(result_df.online_time >= 3000) & (result_df.online_time < 3300)]
    k_df.online_time = '50-55min'
    result_list.append(k_df)

    l_df = result_df[(result_df.online_time >= 3300) & (result_df.online_time < 3600)]
    l_df.online_time = '55-60min'
    result_list.append(l_df)

    m_df = result_df[result_df['online_time'] >= 3600]
    m_df.online_time = '1hmore'
    result_list.append(m_df)


    result_df = pd.concat(result_list)
    result_df = pd.DataFrame(result_df).groupby(['ds', 'online_time']).agg(
        {'user_id': lambda g: g.count()}).reset_index()
    result_df = result_df.rename(
        columns={'user_id': 'user_id_num', })
    result_df.to_excel(r'E:\Data\output\H5\online_time.xlsx', index=False)
    # return result_df


if __name__ == '__main__':
    platform = 'jianniang_test'
    date = '20170524'
    settings_dev.set_env(platform)
    dis_liushi_stay_on_checkpoint_last(date)



