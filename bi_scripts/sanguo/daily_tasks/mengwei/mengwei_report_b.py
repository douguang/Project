#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-3-24 下午3:36
@Author  : Andy 
@File    : mengwei_report_b.py
@Software: PyCharm
Description :  计算玩家的付费情况
'''

from utils import hql_to_df, ds_add, date_range
import settings_dev
import pandas as pd

def mengwei_report_b(platform):
    settings_dev.set_env(platform)
    print platform
    # 新增用户 ds-user_id
    ds_dnu_user_id_sql = '''
                select regexp_replace(substring(reg_time,1,10),'-','') as ds,user_id from mid_info_all where ds='20170323' group by ds,user_id
            '''
    print ds_dnu_user_id_sql
    ds_dnu_user_id_df = hql_to_df(ds_dnu_user_id_sql).fillna(0)
    print ds_dnu_user_id_df.head(3)

    # 每日的首次付费玩家和付费金额
    ds_first_pay_sql = '''
            select t1.user_id,t1.ds,t2.order_money
            from(
              select user_id,min(ds) as ds from raw_paylog  where ds>='20160419' and platform_2 != 'admin_test' group by user_id
            )t1
            left outer join(
              select ds,user_id,sum(order_money)  as order_money from raw_paylog  where ds>='20160419' and platform_2 != 'admin_test' group by ds,user_id
            )t2 on (t1.ds=t2.ds and t1.user_id=t2.user_id)
            group by t1.user_id,t1.ds,t2.order_money
        '''
    print ds_first_pay_sql
    ds_first_pay_df = hql_to_df(ds_first_pay_sql).fillna(0)
    print ds_first_pay_df.head(3)

    # # 新增玩家的新增付费人数和新增付费金额
    result_a_df = ds_dnu_user_id_df.merge(ds_first_pay_df, on=['ds', 'user_id'], how='left')
    result_a_df = result_a_df.dropna()
    result_a_df = result_a_df.groupby(['ds', ]).agg({
        'user_id': lambda g: g.nunique(),
        'order_money': lambda g: g.sum(),
    }).reset_index()
    result_a_df = result_a_df.rename(columns={'user_id': 'first_pay_reg_num', 'order_money': 'first_pay_reg_num_money', })
    print result_a_df.head()
    # # 每天的首次付费人数和付费金额
    result_b_df = ds_first_pay_df.groupby(['ds', ]).agg({
        'user_id': lambda g: g.nunique(),
        'order_money': lambda g: g.sum(),
    }).reset_index()
    result_b_df = result_b_df.rename(columns={'user_id': 'first_pay_num', 'order_money': 'first_pay_money',})
    print result_b_df.head()

    # # 每日付费人数 付费总额
    ds_pay_sql = '''
        select ds,sum(order_money) as order_money,count(distinct user_id) as pun from raw_paylog  where ds>='20160419' and platform_2 != 'admin_test' group by ds
    '''
    print ds_pay_sql
    result_c_df = hql_to_df(ds_pay_sql).fillna(0)
    print result_c_df.head(3)


    result_df = result_c_df.merge(result_b_df, on=['ds',]).fillna(0)
    result_df = result_df.merge(result_a_df, on=['ds',]).fillna(0)
    print result_df.head(3)
    result_df['ds'] = pd.DataFrame((x[0:6] for x in result_df.ds), index=result_df.index,)
    print result_df.head(3)

    result_df = result_df.groupby(['ds',]).agg({
        'order_money': lambda g: g.sum(),
        'pun': lambda g: g.mean(),
        'first_pay_num': lambda g: g.mean(),
        'first_pay_money': lambda g: g.mean(),
        'first_pay_reg_num': lambda g: g.mean(),
        'first_pay_reg_num_money': lambda g: g.mean(),
    }).reset_index()

    result_df.to_excel('/home/kaiqigu/桌面/%s-每天的付费构成.xlsx' % platform,index=False)
if __name__ == '__main__':
    platform = 'sanguo_tw'
    mengwei_report_b(platform)