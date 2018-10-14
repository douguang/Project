#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site: 超英新手引导
@software: PyCharm 
@file: new_user_guide_data.py 
@time: 18/1/15 上午9:52 
"""

import settings_dev
from utils import hql_to_df, ds_add,date_range
from pandas import DataFrame
import pandas as pd

def data_reduce(date):
    act_sql = '''
        SELECT '{date}' as ds,uid,
               args
        FROM
          (SELECT uid,
                  stmp,
                  dense_rank() over(partition BY uid
                                    ORDER BY stmp DESC) AS rn,
                  args
           FROM raw_action_log
           WHERE ds = '{date}'
             AND action = 'user.guide')a
        WHERE rn = 1 and uid in (select uid from raw_info where ds='{date}' and regexp_replace(to_date(create_time),'-','') ='{date}')
     '''.format(date=date, date_last=ds_add(date, 1))
    act_df = hql_to_df(act_sql)
    # reg_sql = '''
    #     SELECT uid
    #     FROM raw_info
    #     WHERE regexp_replace(substr(create_time,1,10),'-','') = '{date}'
    # '''.format(date=date)
    # reg_df = hql_to_df(reg_sql)
    # result = act_df.merge(reg_df, on='uid')
    result = act_df

    dfs = []
    for _, row in result.iterrows():
        print row['args']
        guide_id = 0
        guide_team = 0
        print row.ds
        print row.uid
        try:
            guide_id = eval(row['args'])['guide_id'][0]
            guide_team = eval(row['args'])['guide_team'][0]

        except Exception as ex:
            print ex
            pass

        print guide_team
        print guide_id
        # data = DataFrame({'ds': ,'uid': row.uid,'guide_id': guide_id,'guide_team':guide_team,},)
        dfs.append([row.ds,row.uid,guide_id,guide_team,])

    result_df = pd.DataFrame(dfs,columns=['ds','uid','guide_id','guide_team',])
    print result_df.head()
    result_df['guide_id'] = result_df['guide_id'].map(lambda s: int(s))
    result_df['guide_team'] = result_df['guide_team'].map(lambda s: int(s))

    # result = result_df.groupby(['ds','uid']).max().reset_index()
    guide_id_data = result_df.groupby(['ds', 'guide_team', 'guide_id']).agg(
        {'uid': lambda g: g.nunique(), }).reset_index().rename(columns={'uid': 'guide_id_user_num'})

    return guide_id_data

if __name__ == '__main__':
    platform = 'superhero_bi'
    settings_dev.set_env(platform)
    start_date = '20180101'
    end_date = '20180225'
    res_list = []
    for date in date_range(start_date, end_date):
        res = data_reduce(date)
        res_list.append(res)
    pd.concat(res_list).to_excel('/Users/kaiqigu/Documents/Superhero/超级英雄-国内版-新手引导_20180226.xlsx')