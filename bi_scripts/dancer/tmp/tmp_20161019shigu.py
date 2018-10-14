#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''

'''
import settings_dev
from utils import hqls_to_dfs
from pandas import DataFrame
import pandas as pd


if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')

    # 获取事故UID数据
    df = pd.read_excel("/Users/kaiqigu/Downloads/Excel/shigu_uid.xlsx")

    info_sql = '''
    SELECT distinct user_id
    FROM parse_info
    WHERE ds >='20161004'
      AND ds <='20161011'
    '''
    pay_sql = '''
    SELECT user_id,
           sum(order_money) sum_pay
    FROM raw_paylog
    WHERE ds >='20161004'
      AND ds <='20161011'
    GROUP BY user_id
    '''
    info_df,pay_df = hqls_to_dfs([info_sql,pay_sql])

    info_df['is_shigu_uid'] = info_df['user_id'].isin(df.user_id.values)
    result = info_df[info_df['is_shigu_uid']]
    # 登陆人数
    login_num = result.count().user_id

    result_df = result.merge(pay_df,on='user_id')
    # 充值总额
    sum_pay = result_df.sum().sum_pay

    result_df = DataFrame({'login_num':[login_num],'sum_pay':[sum_pay]})
    result_df['pay_rate'] = result_df['sum_pay']*1.0/result_df['login_num']

    result_df.to_excel('/Users/kaiqigu/Downloads/Excel/shigu.xlsx')
