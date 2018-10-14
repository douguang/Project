#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 活跃用户数据加工表
'''
import settings_dev
from utils import hql_to_df, hqls_to_dfs
from pandas import DataFrame

def ks_beta_ext_activeuser(date):
    sql = '''
    SELECT user_id,
           account,
           reg_time,
           act_time,
           substr(account,1,instr(account,'_')-1) platform
    FROM parse_info
    WHERE ds ='{0}'
     '''.format(date)
    df = hql_to_df(sql)
    return df

def tw_ext_activeuser(date):
    act_sql = '''
    SELECT user_id,
           level,
           account,
           a_tar
    FROM mid_actionlog
    WHERE ds ='{0}'
    '''.format(date)
    info_sql = '''
    SELECT user_id,
           account,
           reg_time,
           act_time
    FROM raw_12ago_info
    WHERE ds ='{0}'
    '''.format(date)
    info_df,act_df = hqls_to_dfs([info_sql,act_sql])

    user_id_list,appid_list = [],[]
    for _, row in act_df.iterrows():
        args = eval(row['a_tar'])
        if args.has_key('appid'):
            appid = args['appid']
        else:
            appid = ' '
        user_id_list.append(row['user_id'])
        appid_list.append(appid)

    result_df = DataFrame({'user_id':user_id_list,'platform':appid_list})
    uid_info_df = result_df.groupby(['user_id']).max().platform.reset_index()
    result = info_df.merge(uid_info_df,on = 'user_id',how = 'left')

    columns = ['user_id', 'account','reg_time', 'act_time','platform']
    result = result[columns]

    return result

def dis_ext_activeuser(date):
    plat=settings_dev.code
    if plat == 'dancer_ks_beta':
        df = ks_beta_ext_activeuser(date)
    else:
        df = tw_ext_activeuser(date)

    # 导出数据到指定文件
    df.to_csv('/home/data/{plat}/redis_stats/ext_activeuser_{date}'.format(
        plat=plat, date=date),
                  sep='\t',
                  index=False,
                  head=False)


if __name__ == '__main__':
    settings_dev.set_env('dancer_ks_beta')
    date = '20190913'
    dis_ext_activeuser(date)
