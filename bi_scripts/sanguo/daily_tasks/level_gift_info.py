#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: level_gift_info.py 
@time: 18/1/31 下午7:09 
"""

import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range
import pandas as pd

def data_reduce():
    info_sql = '''
        select ds,account,user_id,vip_level,a_typ,a_tar from parse_actionlog 
        where ds>='20180129' and a_typ = 'user.get_level_gift' and user_id in (select user_id from mid_info_all where ds='20180129' and to_date(reg_time)='2018-01-29')
        group by ds,account,user_id,vip_level,a_typ,a_tar
    '''
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head()

    def card_evo_lines():
        for _, row in info_df.iterrows():
            lv_gift = eval(row.a_tar).get('lv', '')
            yield [row.ds,row.account,row.user_id,row.vip_level,row.a_typ,row.a_tar,int(lv_gift)]

    act_all_df = pd.DataFrame(card_evo_lines(), columns=['ds', 'account','user_id','vip_level','a_typ','a_tar','level',])
    act_all_df['level_gift'] = 'level_gift'
    # act_all_df.to_excel(r'/Users/kaiqigu/Documents/Sanguo/合金装甲-测试版-等级礼包-1_20180201.xlsx', index=False)

    plat_result = act_all_df.groupby(['level',]).agg({
        'account': lambda g: g.nunique(),
    }).reset_index().rename(columns={'account': 'account_num', })

    plat_result.to_excel(r'/Users/kaiqigu/Documents/Sanguo/合金装甲-测试版-等级礼包_20180201.xlsx', index=False)

    vip_info_sql = '''
        select account,user_id,vip,level from mid_info_all where ds='20180131' and to_date(reg_time) = '2018-01-29' group by account,user_id,vip,level
    '''
    print vip_info_sql
    vip_df = hql_to_df(vip_info_sql)
    print vip_df.head()

    vip_df['is_shui'] = vip_df['user_id'].isin(info_df.user_id.values)
    vip_result = vip_df.groupby(['is_shui', 'level', ]).agg({
        'account': lambda g: g.nunique(),
    }).reset_index().rename(columns={'account': 'account_num', })

    vip_result.to_excel(r'/Users/kaiqigu/Documents/Sanguo/1合金装甲-测试版-level中购买等级礼包_20180131.xlsx', index=False)

    fisrt_pay_sql = '''
        select t1.user_id,t1.level,'fisrt_pay' as fisrt_pay from (
        select user_id,level,row_number() over(partition by user_id order by order_time ) as rn from raw_paylog where ds>='20180129' and platform_2!='admin_test'
        )t1
        where t1.rn =1
    '''
    print fisrt_pay_sql
    fisrt_pay_df = hql_to_df(fisrt_pay_sql)
    print fisrt_pay_df.head()

    vip_df['first_pay'] = vip_df['user_id'].isin(info_df.user_id.values)
    vip_2result = vip_df.groupby(['level','first_pay']).agg({
        'account': lambda g: g.nunique(),
    }).reset_index().rename(columns={'account': 'account_first_pay_num', })
    vip_3result = act_all_df.groupby(['level',]).agg({
        'account': lambda g: g.nunique(),
    }).reset_index().rename(columns={'account': 'account_level_gift_num', })
    vip_4result = vip_2result.merge(vip_3result, on='level', how='left')
    vip_4result.to_excel(r'/Users/kaiqigu/Documents/Sanguo/2合金装甲-测试版-level中中购买等级礼包及首次付费_20180131.xlsx', index=False)




    return info_df

if __name__ == '__main__':
    #dis_kaiqigu_pay_detail('20170102')
    for platform in ['metal_beta',]:
        settings_dev.set_env(platform)
        data_reduce()
    print "end"

