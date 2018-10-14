#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  装备重铸
@software: PyCharm 
@file: equip_recast.py 
@time: 17/12/19 下午3:31

装备重铸 equip.equip_recast
重铸保留 equip.recast_save
兑换礼物 equip.recast_exchange

"""

from utils import hqls_to_dfs, ds_add, hql_to_df, date_range
import settings_dev
import pandas as pd
from pandas import DataFrame
from ipip import IP


def data_reduce(start_ds,end_ds):
    print start_ds,end_ds
    info_sql = '''
        select ds,uid,action,args,act_time from raw_action_log where ds>='{start_ds}' and ds<='{end_ds}' and action in ('equip.equip_recast','equip.recast_save','equip.recast_exchange') group by ds,uid,action,args,act_time
    '''.format(start_ds=start_ds,end_ds=end_ds)
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df

    def plat_lines():
        for _, row in info_df.iterrows():
            args_type = row.args
            count = ''
            equip_id = ''
            recast_id = ''
            exchange_id = ''
            if 'count' in args_type:
                count = eval(args_type)['count'][0]
                print eval(args_type)['count'][0]
            if 'equip_id' in args_type:
                equip_id = eval(args_type)['equip_id'][0]
                print eval(args_type)['equip_id'][0]
            if 'recast_id' in args_type:
                recast_id = eval(args_type)['recast_id'][0]
                print eval(args_type)['recast_id'][0]
            if 'exchange_id' in args_type:
                exchange_id = eval(args_type)['exchange_id'][0]
                print eval(args_type)['exchange_id'][0]

            yield [row.ds,row.uid,row.action,count,recast_id,equip_id,exchange_id,row.act_time,]

    res_df = pd.DataFrame(plat_lines(), columns=['ds', 'uid', 'action','count','recast_id', 'equip_id','exchange_id','act_time',])
    print res_df.head()

    # 混合碳晶总消耗数量 高级锻造石兑换混合碳晶次数 超级能晶兑换混合碳晶次数 钻石兑换混合碳晶次数
    result_df = res_df.groupby(['ds', 'action', 'exchange_id', 'count','recast_id','equip_id']).agg({
        'uid': lambda g: g.nunique(),
        'act_time': lambda g: g.count(),
    }).reset_index().rename(columns={'uid': 'uid_num', 'act_time': 'num', })


    return res_df,result_df


if __name__ == '__main__':
    platform = 'superhero_bi'
    settings_dev.set_env(platform)
    start_date = '20171118'
    end_date = '20171218'
    res1,res2 = data_reduce(start_date, end_date,)
    res1.to_excel('/Users/kaiqigu/Documents/Superhero/%s-装备重铸数据1_20171219.xlsx' % platform)
    res2.to_excel('/Users/kaiqigu/Documents/Superhero/%s-装备重铸数据2_20171219.xlsx' % platform)
