#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-13 上午10:39
@Author  : Andy 
@File    : new_user_guide_lose_point_last_step.py
@Software: PyCharm
Description :  新手引导流失停留
'''

from utils import hql_to_df, get_server_list, ds_add, date_range, format_dates, update_mysql
import pandas as pd
import datetime
import settings
from pandas import DataFrame

def get_data(date):
    sql = '''
            select t1.user_id,t1.reg_ds,t2.a_tar,t2.log_t
                    from (
                       select user_id,regexp_replace(substr(reg_time,1,10),'-','')  as reg_ds
                         from mid_info_all
                         where regexp_replace(substr(reg_time,1,10),'-','') ="{date}"
                         and user_id  not in (
                           select user_id
                           from mid_actionlog
                           where ds='{tomorrow}'
                           group by user_id
                         )
                        group by user_id,regexp_replace(substr(reg_time,1,10),'-','')
                     )t1
                    left outer join(
                        select user_id,a_tar,max(log_t) as log_t
                        from parse_actionlog
                        where a_typ='user.guide'
                        and ds="{date}"
                        and return_code=''
                        and a_tar != ''
                        group by user_id,a_tar
                    )t2 on t1.user_id = t2.user_id
            group by  t1.user_id,t1.reg_ds,t2.a_tar,t2.log_t
            order by t1.user_id,t1.reg_ds,t2.a_tar,t2.log_t
    '''.format(date=date,tomorrow=ds_add(date,+1))
    print sql
    df = hql_to_df(sql)
    df = df.dropna()
    df = pd.DataFrame(df).reindex()
    print df

    last_step={}
    user_t={}
    a = len(df)
    for i in range(a):
        user_id = df.iloc[i, 0]
        tar = df.iloc[i, 2]
        tar = eval(tar)
        guide_id = tar['guide_id']
        log_t = df.iloc[i, 3]

        key = user_id
        value=guide_id
        if key in last_step:
            if log_t >= user_t[key]:
                last_step[key] = value
        else:
            last_step[key] = value
            user_t[key] = log_t

    user_id_list=[]
    guide_id_list=[]
    for key, value in last_step.items():
        user_id_list.append(key)
        guide_id_list.append(value)

    df = pd.DataFrame({
          '用户ID': user_id_list,
          '引导步骤': guide_id_list
         })
    print df
    return df

def reduce_data(start_ds,end_ds):
    results = []
    for i in date_range(start_ds, end_ds):
        result = get_data(i)
        results.append(result)
    final_df = pd.concat(results)
    final_df = pd.DataFrame(final_df).reindex()
    #  计算总流失人数
    mid_df = final_df['用户ID']
    mid_df = mid_df.drop_duplicates()
    print type(mid_df)

    mid_len = mid_df.count()
    #  计算步骤流失人数
    final_df = final_df.groupby(['引导步骤',]).agg(
        {'用户ID': lambda g: g.nunique()}).reset_index()
    final_df = final_df.rename(columns={'用户ID': '到达人数', })

    final_df['流失率'] = final_df['到达人数']/mid_len

    return final_df


if __name__ == '__main__':
    for platform in ['sanguo_kr']:
        settings.set_env(platform)
        final_df = reduce_data("20161207","20161211")
        final_df.to_excel('/home/kaiqigu/桌面/新手引导流失_最后一步.xlsx', index=False)
    print "end"