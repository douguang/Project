#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  VIP等级礼包
@software: PyCharm 
@file: shop_vip_buy_data.py 
@time: 18/2/8 下午9:32 
"""


import settings_dev
from utils import hql_to_df, ds_add,date_range
from pandas import DataFrame
import pandas as pd

def data_reduce(date):
    act_sql = '''
        select ds,uid,to_date(subtime) as act_ds,args from raw_spendlog where ds>='20180314' and goods_type='shop.vip_buy' 
     '''.format(date=date, date_last=ds_add(date, 1))
    act_df = hql_to_df(act_sql)

    result = act_df

    dfs = []
    for _, row in result.iterrows():
        print row['args']
        try:
            shop_id = eval(row['args'])['shop_id'][0]
            dfs.append([row.ds, row.uid, shop_id, ])
            print shop_id

        except Exception as ex:
            print ex
            pass


        # data = DataFrame({'ds': ,'uid': row.uid,'guide_id': guide_id,'guide_team':guide_team,},)


    result_df = pd.DataFrame(dfs,columns=['ds','uid','shop_id',])
    print result_df.head()
    result_df['shop_id'] = result_df['shop_id'].map(lambda s: int(s))

    # admin_test_user_sql = '''
    #     select uid from raw_paylog where ds>='20180206' and platform_2 = 'admin_test' group by uid
    # '''
    # admin_test_df = hql_to_df(admin_test_user_sql)
    #
    # result_df = result_df[~result_df['uid'].isin(set(admin_test_df['uid']))]
    # result_df['guide_team'] = result_df['guide_team'].map(lambda s: int(s))

    # result = result_df.groupby(['ds','uid']).max().reset_index()
    # guide_id_data = result_df.groupby(['ds', 'guide_team', 'guide_id']).agg(
    #     {'uid': lambda g: g.nunique(), }).reset_index().rename(columns={'uid': 'guide_id_user_num'})


    return result_df

if __name__ == '__main__':
    platform = 'superhero_mul'
    settings_dev.set_env(platform)
    start_date = '20180206'
    end_date = '20180208'
    res_list = []
    for date in date_range(start_date, end_date):
        res = data_reduce(date)
        res_list.append(res)
    pd.concat(res_list).to_excel('/Users/kaiqigu/Documents/Superhero/超级英雄-英文版-等级礼包2_20180209.xlsx')