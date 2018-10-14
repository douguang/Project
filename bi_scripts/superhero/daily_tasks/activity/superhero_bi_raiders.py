#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  一元夺宝 国内
@software: PyCharm 
@file: superhero_bi_raiders.py 
@time: 17/12/27 上午10:42 
"""

from utils import hqls_to_dfs, ds_add, hql_to_df, date_range
import settings_dev
import pandas as pd
from pandas import DataFrame


def data_reduce(start_ds,end_ds):
    dau_sql = '''
        select ds,count(distinct account) as dau from raw_info where ds>='{start_ds}' and ds<='{end_ds}' group by ds
    '''.format(start_ds=start_ds,end_ds=end_ds)
    print dau_sql
    dau_df = hql_to_df(dau_sql)
    print dau_df.head()

    canyu_sql = '''
        select t3.ds,count(distinct t3.account)as player_num from (
        select t1.ds,t1.uid,t2.account from (
        select ds,uid from raw_action_log where ds>='{start_ds}' and ds<='{end_ds}' and action like '%raiders%' and action !='raiders.index'
        )t1
        left outer join(
          select uid,account from mid_info_all where ds='{end_ds}'  group by uid,account
          )t2 on t1.uid=t2.uid
          group by t1.ds,t1.uid,t2.account
        )t3
        group by t3.ds
    '''.format(start_ds=start_ds,end_ds=end_ds)
    print canyu_sql
    canyu_df = hql_to_df(canyu_sql)
    print canyu_df.head()

    activity_spend_sql = '''
        select ds,sum(coin_num) as coin_num from raw_spendlog where ds>='{start_ds}' and ds<='{end_ds}' and goods_type like '%raiders%'  group by ds
    '''.format(start_ds=start_ds,end_ds=end_ds)
    print activity_spend_sql
    activity_spend_df = hql_to_df(activity_spend_sql)
    print activity_spend_df.head()


    actionlog_sql = '''
      select t1.ds,t1.uid,t1.action,t1.args,t2.account from(
        select ds,uid,action,args from raw_action_log where ds>='{start_ds}' and ds<='{end_ds}'  and  action ='raiders.open_raiders'  group by ds,uid,action,args
      )t1
      left outer join(
        select uid,account from mid_info_all where ds='{end_ds}'  group by uid,account
      )t2 on t1.uid=t2.uid
    '''.format(start_ds=start_ds,end_ds=end_ds)
    print actionlog_sql
    spend_info_df = hql_to_df(actionlog_sql)
    print spend_info_df.head()

    def plat_lines():
        for _, row in spend_info_df.iterrows():
            args_type = row.args
            # print eval(args_type)['type'][0]
            number = int(eval(args_type)['number'][0])
            num = eval(args_type)['num'][0]
            yield [row.ds,row.uid,row.action,row.account,num,number]

    spend_df = pd.DataFrame(plat_lines(), columns=['ds', 'uid', 'action','account', 'num', 'number',])
    print spend_df.head()
    spend_df = spend_df.groupby(['ds', 'num',]).agg({
        'account': lambda g: g.nunique(),
        'number': lambda g: g.sum(),
    }).reset_index().rename(columns={'account': 'account_num',})


    activ_spernd_top3_sql = '''
        select t1.* from(
        select ds,uid,sum(coin_num) as top_3_coin_num,row_number() over(partition by ds order by sum(coin_num) desc)  as rank from raw_spendlog where ds>='{start_ds}' and ds<='{end_ds}' and goods_type like '%raiders%'  group by ds,uid
        )t1
        where t1.rank<=3
    '''.format(start_ds=start_ds,end_ds=end_ds)
    print activ_spernd_top3_sql
    spend_info_df = hql_to_df(activ_spernd_top3_sql)
    spend_info_df = spend_info_df[['ds','top_3_coin_num']]
    print spend_info_df.head()



    result_df = canyu_df.merge(dau_df, on=['ds',], how='left')
    result_df = result_df.merge(activity_spend_df, on=['ds',], how='left')
    result_df = result_df.merge(spend_df, on=['ds',], how='left')
    result_df = result_df.merge(spend_info_df, on=['ds',], how='left')

    return result_df


if __name__ == '__main__':
    platform = 'superhero_bi'
    start_ds = '20171009'
    end_ds = '20171226'
    settings_dev.set_env(platform)
    res = data_reduce(start_ds, end_ds)
    pd.DataFrame(res).to_excel('/Users/kaiqigu/Documents/Superhero/超级英雄-国内-一元夺宝活动参与_20171227-2.xlsx', index=False)

    print 'end '

