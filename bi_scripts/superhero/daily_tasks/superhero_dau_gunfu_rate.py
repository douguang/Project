#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: superhero_dau_gunfu_rate.py
@time: 17/9/1 下午3:30 
"""

#!/usr/bin/env python
# encoding: utf-8


import settings_dev
from utils import ds_add
from utils import hql_to_df
from utils import update_mysql
from sqls_for_games.superhero import gs_sql
import pandas as pd


def data_reduce():
    # 是否是
    is_not_gunfu_sql = '''
        select t2.account,t2.uid,t2.reg_ds,t2.rank,case when t2.rank=1 then 'fisrt' else 'duokai' end as is_not_duokai from(
            select t1.account,row_number() over( partition by  t1.account  order by min(t1.reg_ds)  ASC ) as rank,t1.uid,t1.reg_ds from (
                  select account,uid,regexp_replace(substring(create_time,1,10),'-','') as reg_ds from mid_info_all where ds='20170831' group by account,uid,reg_ds
            )t1
            where account != ''
            group by t1.account,t1.uid,t1.reg_ds
            order by t1.account,t1.uid,t1.reg_ds
        )t2
        group by t2.account,t2.uid,t2.reg_ds,t2.rank,is_not_duokai
    '''
    is_not_gunfu_df = hql_to_df(is_not_gunfu_sql)
    print is_not_gunfu_df.head(3)
    # is_not_gunfu_mid_df = is_not_gunfu_df.groupby(['reg_ds',]).agg(
    #     {'uid': lambda g: g.nunique()}).reset_index()
    # is_not_gunfu_mid_df.to_excel('/Users/kaiqigu/Documents/Sanguo/xinlaofu_20170901.xlsx')


    #   新老服
    is_old_or_new_sql = '''
        select t1.ds,t1.server,t2.reg_ds as server_first,datediff(from_unixtime(unix_timestamp(t1.ds,'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(t2.reg_ds,'yyyyMMdd'),'yyyy-MM-dd'))+1 as date_diff from (
          select ds,reverse(substring(reverse(uid),8)) as server from raw_info where ds>='20170101' group by ds,server
        ) t1 left outer join(
          select reverse(substring(reverse(uid),8)) as server,min(regexp_replace(to_date(create_time),'-','')) as reg_ds from mid_info_all where ds='20170831' group by server 
        )t2 on t1.server=t2.server
        group by t1.ds,t1.server,t2.reg_ds,date_diff
    '''
    is_old_or_new_df = hql_to_df(is_old_or_new_sql)
    is_old_or_new_df =is_old_or_new_df[is_old_or_new_df['ds']>'20170601']
    print is_old_or_new_df.head(3)
    # is_old_or_new_mid_df = is_old_or_new_df.groupby(['ds',]).agg(
    #     {'dau': lambda g: g.sum()}).reset_index()
    # is_old_or_new_mid_df.to_excel('/Users/kaiqigu/Documents/Sanguo/xiallaofu3_20170901.xlsx')
    # 每天登陆用户    计算——每日DAU中多开账号的数量
    ds_act_sql = '''
        select ds,reverse(substring(reverse(uid),8)) as server,uid from raw_info where ds>='20170101' group by ds,server,uid
    '''
    ds_act_df = hql_to_df(ds_act_sql)
    print ds_act_df.head(3)
    mid_si_gun = is_not_gunfu_df[['uid','is_not_duokai',]].drop_duplicates()
    ds_act_num_df = ds_act_df.merge(mid_si_gun, on=['uid', ], how='left')
    ds_act_num_df = ds_act_num_df.groupby(['ds', 'server','is_not_duokai']).agg({'uid': lambda g: g.nunique()}).reset_index()
    ds_act_num_df = ds_act_num_df.rename(columns={'uid': 'duokai_num', })
    print ds_act_num_df.head(3)


    dau_sql = '''
        select ds,reverse(substring(reverse(uid),8)) as server,count(distinct uid) as dau from raw_info where ds>='20170101' group by ds,server
    '''
    dau_df = hql_to_df(dau_sql)
    print dau_df.head(3)

    result = is_old_or_new_df.merge(ds_act_num_df, on=['ds', 'server',], how='left')
    result = result.merge(dau_df, on=['ds', 'server',], how='left')
    # result = (result.pivot_table('duokai_num', ['ds', 'server','dau','server_first','date_diff'],
    #                                         'is_not_duokai').reset_index().fillna(0))
    print result.head(3)


    result.to_excel('/Users/kaiqigu/Documents/Sanguo/超级英雄-BI-DAU及滚服数据_20170901.xlsx')


if __name__ == '__main__':
    settings_dev.set_env('superhero_bi')
    data_reduce()