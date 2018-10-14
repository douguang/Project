#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  秘境探宝-消耗钻石和消耗道具
@software: PyCharm 
@file: superhero_vt_new_roulette.py 
@time: 17/12/18 下午3:31 
"""


from utils import hqls_to_dfs, ds_add, hql_to_df, date_range
import settings_dev
import pandas as pd
from pandas import DataFrame


def data_reduce(date):
    spend_info_sql = '''
      select ds,uid,goods_type,args,subtime,goods_price,goods_num,goods_name,coin_num from raw_spendlog  where ds='{date}' and goods_type like '%new_roulette%' group by ds,uid,goods_type,args,subtime,goods_price,goods_num,goods_name,coin_num
    '''.format(date=date)
    print spend_info_sql
    spend_info_df = hql_to_df(spend_info_sql)
    print spend_info_df.head()

    def plat_lines():
        for _, row in spend_info_df.iterrows():
            args_type = row.args
            # print eval(args_type)['type'][0]
            args_type = eval(args_type)['type'][0]
            yield [row.ds,row.uid,row.goods_type,args_type,row.subtime,row.goods_price,row.goods_num,row.goods_name,row.coin_num]

    spend_df = pd.DataFrame(plat_lines(), columns=['ds', 'uid', 'goods_type','args_type', 'subtime', 'goods_price','goods_num', 'goods_name', 'coin_num',])
    print spend_df.head()

    act_info_sql = '''
        select ds,account,uid,max(vip_level) as vip from raw_info where ds='{date}' group by ds,account,uid
    '''.format(date=date)
    print act_info_sql
    act_info_df = hql_to_df(act_info_sql)
    print act_info_df.head()

    result_df = spend_df.merge(act_info_df, on=['ds', 'uid', ], how='left')
    result_df = result_df.groupby(['ds','args_type','coin_num','vip']).agg({
        'uid': lambda g: g.nunique(),
        'account': lambda g: g.nunique(),
    }).reset_index().rename(columns={'uid': 'uid_num', 'account': 'account_num',})

    return result_df


if __name__ == '__main__':
    res_list = []
    platform = 'superhero_vt'
    for date in ['20171201','20171107','20171121',]:
        settings_dev.set_env(platform)
        res = data_reduce(date)
        res_list.append(res)
    pd.concat(res_list).to_excel('/Users/kaiqigu/Documents/Superhero/超级英雄-越南-秘境探宝活动参与_20171218.xlsx', index=False)

    print 'end '

