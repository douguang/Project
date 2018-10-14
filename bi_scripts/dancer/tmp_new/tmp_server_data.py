#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 武娘多语言 - 日常数据
create_date : 2016.07.17
Illustration: 日期    服   DAU 充值人数    充值金额    arpu    arppu
'''
import settings_dev
import numpy as np
from utils import hqls_to_dfs


def get_div(df, res_col, div_chu, div_beichu):
    if div_beichu == 0:
        return 0
    else:
        df[res_col] = df[div_chu] * 1.0 / df[div_beichu]
        return df


if __name__ == '__main__':
    settings_dev.set_env('dancer_mul')
    # 充值数据
    pay_sql = '''
    SELECT ds,
           reverse(substr(reverse(user_id),8)) AS server,
           count(distinct user_id) AS pay_num,
           sum(order_money) AS pay_money
    FROM raw_paylog
    WHERE platform_2 <> 'admin_test'
      AND order_id NOT LIKE '%test%'
    GROUP BY ds,
             reverse(substr(reverse(user_id),8))
    '''
    # 活跃数据
    info_sql = '''
    SELECT ds,
           reverse(substr(reverse(user_id),8)) AS server,
           count(user_id) AS dau
    FROM parse_info
    GROUP BY ds,
             reverse(substr(reverse(user_id),8))
    '''
    pay_df, info_df = hqls_to_dfs([pay_sql, info_sql])

    result_df = info_df.merge(pay_df, on=['ds', 'server'], how='outer')

    # arpu、arppu
    result_df = get_div(result_df, 'arpu', 'pay_money', 'dau')
    result_df = get_div(result_df, 'arppu', 'pay_money', 'pay_num')
    result_df = result_df.replace({np.inf: 0})

    result_df.to_excel(r'/Users/kaiqigu/Documents/Excel/server_data.xlsx')
