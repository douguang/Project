#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-3-20 下午3:01
@Author  : Andy 
@File    : dis_liushi_stay_on_action_last.py
@Software: PyCharm
Description :   次日流失用户的最后动作停留
'''

from utils import hql_to_df, ds_add, date_range
import settings_dev
import pandas as pd

def dis_liushi_stay_on_action_last(date):
    print date
    # 流失用户
    ciliushi_sql = '''
        select ds,user_id from raw_info where ds='{date}' and regexp_replace(substring(reg_time,1,10),'-','')  = '{date}'  and user_id not in (select user_id from raw_info where ds='{tomor_date}' group by user_id)  group by ds,user_id
    '''.format(date=date,tomor_date=ds_add(date,1))
    print ciliushi_sql
    ciliushi_df = hql_to_df(ciliushi_sql).fillna(0)
    print ciliushi_df.head(3)

    # 用户当日的最后行为
    last_act_atyp_sql='''
        select t1.ds,t1.user_id,t2.a_typ from (
        select ds,user_id,max(unix_timestamp(log_t,'yyyy-MM-dd HH:mm:ss')) as ts from parse_actionlog where ds='{date}' group by ds,user_id order by ds,user_id
        )t1
        left outer join(
          select ds,user_id,max(unix_timestamp(log_t,'yyyy-MM-dd HH:mm:ss')) as ts,a_typ,log_t from parse_actionlog where ds='{date}' group by ds,user_id,a_typ,log_t order by ds,user_id,a_typ,log_t
          )t2 on (t1.user_id=t2.user_id and t1.ds=t2.ds and t1.ts=t2.ts)
        group by t1.ds,t1.user_id,t2.a_typ
        order by t1.ds,t1.user_id,t2.a_typ
    '''.format(date=date)
    print last_act_atyp_sql
    last_act_atyp_df = hql_to_df(last_act_atyp_sql).fillna(0)
    print last_act_atyp_df.head(3)

    result_df = ciliushi_df.merge(last_act_atyp_df,on=['ds','user_id',])
    result_df = pd.DataFrame(result_df).groupby(['ds','a_typ']).agg(
        {'user_id': lambda g: g.nunique()}).reset_index()

    result_df = result_df.rename(
        columns={'user_id': 'user_id_num', })
    print result_df.head(3)

    result_df.to_excel(r'E:\Data\output\H5\last_action.xlsx', index=False)
if __name__ == '__main__':
    platform = 'jianniang_test'
    date = '20170524'
    settings_dev.set_env(platform)
    dis_liushi_stay_on_action_last(date)

