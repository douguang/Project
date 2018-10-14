#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 武娘 新增注册、次留、三留、7留、14留、30留、充值金额、付费人数、DAU
create_date : 2016.07.17
'''
from utils import hql_to_df, date_range, ds_add
import settings_dev
import pandas as pd
import numpy as np

def tmp_20170321_platform_data(start_date, date):

    #常规和充值数据
    dau_sql = '''
        select count(distinct account) as dau, ds from parse_info where ds>='{start_date}' and ds<='{date}' and account in (
            select distinct account from parse_nginx where ds>='{start_date}' and ds<='{date}' and appid='cnwnioswnhd'
        ) group by ds
    '''.format(start_date=start_date, date=date)
    print dau_sql
    info_df = hql_to_df(dau_sql)
    print info_df.head(10)

    reg_sql = '''
        select count(distinct account) as reg_num, regexp_replace(to_date(reg_time), '-', '') as ds from mid_info_all where ds='{date}'
        and regexp_replace(to_date(reg_time), '-', '')>='{start_date}' and regexp_replace(to_date(reg_time), '-', '')<='{date}' and account in (
            select distinct account from parse_nginx where ds>='{start_date}' and ds<='{date}' and method='new_user' and appid='cnwnioswnhd'
        )  group by  regexp_replace(to_date(reg_time), '-', '')
    '''.format(start_date=start_date, date=date, start_date_before=ds_add(start_date, -1))
    reg_df = hql_to_df(reg_sql)
    print reg_df.head(10)
    info_df = info_df.merge(reg_df, on='ds', how='left')
    # info_df.to_excel(r'E:\Data\output\dancer\meizu_info.xlsx')

    #充值数据
    pay_sql = '''
        select pay.ds,
           pay.user_id,
           account,
           pay_rmb
    from
    (
        select ds,
               user_id,
               sum(order_money) as pay_rmb
        from raw_paylog
        where platform_2 != 'admin_test' and ds>='{start_date}' and ds<='{date}'
        AND order_id not like '%testktwwn%'
        group by ds, user_id
    ) pay
    left join
    (
        select user_id, account, ds
        from parse_info
        where ds>='{start_date}' and ds<='{date}'
    ) info on (info.user_id = pay.user_id and info.ds=pay.ds)
    '''.format(start_date=start_date, date=date)
    print pay_sql
    pay_df = hql_to_df(pay_sql)
    print pay_df.head(10)
    account_sql = '''
        select distinct account from parse_nginx where ds>='{start_date}' and ds<='{date}' and method='new_user' and appid='cnwnioswnhd'
    '''.format(start_date=start_date, date=date)
    account_df = hql_to_df(account_sql)
    print account_df.head(10)
    pay_df = pay_df[pay_df['account'].isin(account_df['account'])]
    user_id_sql = '''
        select t1.user_id, t1.pay, t1.ds from
        (select user_id, sum(order_money) as pay, ds from raw_paylog where ds>='{start_date}' and ds<='{date}' and platform_2 != 'admin_test' and order_id not like '%testktwwn%' and user_id in
        (select user_id from mid_info_all where ds='{date}' and  regexp_replace(to_date(reg_time), '-', '')>='{start_date}' and level<=8) group by user_id, ds) t1 where t1.pay>=2000
    '''.format(start_date=start_date, date=date)
    print user_id_sql
    user_id_df = hql_to_df(user_id_sql)
    pay_df = pay_df[~pay_df['user_id'].isin(user_id_df['user_id'])]
    pay_df = pay_df.groupby('ds').agg({'account': lambda g: g.nunique(), 'pay_rmb': lambda g: g.sum()}).reset_index()
    print pay_df.head(10)

    result = info_df.merge(pay_df, on='ds', how='left')
    print result.head(10)
    return result




if __name__ == '__main__':
    settings_dev.set_env('dancer_pub')
    # result_list = []
    # for date in date_range('20170329', '20170404'):
    #     result_list.append(tmp_20170321_platform_data(date))
    # result = pd.concat(result_list)
    result = tmp_20170321_platform_data('20170329', '20170406')
    result.to_excel(r'E:\Data\output\dancer\appid_data_new.xlsx')