#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  新手引导之前的打点
@software: PyCharm 
@file: new_user_guide_before_data.py 
@time: 18/1/31 下午6:13 
"""


import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range
import pandas as pd

def data_reduce():

    nginx_sql = '''
    select user_token as user_id,api_type,method, account  from parse_nginx where ds='20180129' and api_type != 'admin' group by user_id,api_type,method, account
    '''
    print nginx_sql
    nginx_df = hql_to_df(nginx_sql)
    print nginx_df.head()

    info_sql = '''
        select user_id,account,to_date(reg_time)  as reg_ds from raw_info where ds='20180129' group by user_id,account,reg_ds
    '''
    print info_sql

    info_df = hql_to_df(info_sql)
    print info_df.head()
    userinfo_dict = {a.strip():b.strip() for a, b in info_df[['user_id','account']].get_values()}
    accountinfo_dict = {b.strip():a.strip()for a, b in info_df[['user_id','account']].get_values()}
    print userinfo_dict
    print accountinfo_dict

    def card_evo_lines():
        for _, row in nginx_df.iterrows():
            user_id = row.user_id
            api_type = row.api_type
            method = row.method
            account = row.account
            if user_id  == '' and account  != '':
                user_id = accountinfo_dict.get(account, '')
            if account  == '' and user_id != '':
                account = userinfo_dict.get(user_id, '')
            yield [user_id, api_type, method, account,]

    nginx_all_df = pd.DataFrame(card_evo_lines(), columns=['user_id', 'api_type','method','account',])

    result = nginx_all_df.merge(info_df, on=['user_id', 'account', ], how='left')

    result.to_excel(r'/Users/kaiqigu/Documents/Sanguo/合金装甲-测试版-打点数据-1_20180131.xlsx', index=False)

    plat_result = result.groupby(['reg_ds','method',]).agg({
        'account': lambda g: g.nunique(),
    }).reset_index().rename(columns={'account': 'account_num', })

    plat_result.to_excel(r'/Users/kaiqigu/Documents/Sanguo/合金装甲-测试版-打点数据-2_20180131.xlsx', index=False)




    return info_df

if __name__ == '__main__':
    #dis_kaiqigu_pay_detail('20170102')
    for platform in ['metal_beta',]:
        settings_dev.set_env(platform)
        data_reduce()
    print "end"

