#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  VIP等級礼包
@software: PyCharm 
@file: shop_vip_buy.py 
@time: 18/3/15 下午4:25 
"""
import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range
import pandas as pd

import settings_dev
from utils import hql_to_df, ds_add,date_range
from pandas import DataFrame
import pandas as pd

def data_reduce(date):
    info_sql = '''
            select ds,account,user_id,vip_level,a_typ,a_tar from parse_actionlog 
        where ds='20180314' and a_typ = 'shop.vip_buy' 
        group by ds,account,user_id,vip_level,a_typ,a_tar
    '''
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head()

    def card_evo_lines():
        for _, row in info_df.iterrows():
            shop_id = eval(row.a_tar).get('shop_id', '')
            yield [row.ds, row.account, row.user_id, row.vip_level, row.a_typ,int(shop_id)+1]

    act_all_df = pd.DataFrame(card_evo_lines(),columns=['ds', 'account', 'user_id', 'vip_level', 'a_typ','shop_id', ])

    act_all_df = act_all_df.groupby(['shop_id',]).agg(
        {'user_id': lambda g: g.nunique(), }).reset_index().rename(columns={'shop_id': 'vip','user_id': 'num'})

    # act_all_df.to_excel('/Users/kaiqigu/Documents/Sanguo/合金装甲-国内版-VIP等级礼包——shop_id_20180315.xlsx')

    vip_user_sql = '''
        select vip,count(distinct user_id) as vip_num from raw_info where  ds='20180314' group by vip
    '''
    vip_df = hql_to_df(vip_user_sql)
    print vip_df.head()
    # vip_df.to_excel('/Users/kaiqigu/Documents/Sanguo/合金装甲-国内版-VIP等级礼包——vip_df_20180315.xlsx')

    res = vip_df.merge(act_all_df,on=['vip',])

    return res

if __name__ == '__main__':
    platform = 'metal_pub'
    settings_dev.set_env(platform)
    start_date = '20180314'
    end_date = '20180314'
    res_list = []
    for date in date_range(start_date, end_date):
        res = data_reduce(date)
        res_list.append(res)
    pd.concat(res_list).to_excel('/Users/kaiqigu/Documents/Sanguo/合金装甲-国内版-VIP等级礼包2_20180315.xlsx')

