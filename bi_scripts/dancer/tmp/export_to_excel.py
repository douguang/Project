#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
Time        :
illustration:
'''
import settings_dev
from utils import ds_add
from utils import sql_to_df


if __name__ == '__main__':
    settings_dev.set_env('dancer_pub')
    end_date = '20170514'
    start_date = ds_add(end_date, -14)
    sql = '''
    SELECT *
    FROM dis_platform_ltv
    WHERE ds >= '{start_date}'
    and ds <= '{end_date}'
    ORDER BY ds
    '''.format(start_date=start_date, end_date=end_date)
    df = sql_to_df(sql)
    df.to_excel('/Users/kaiqigu/Documents/Excel/platform_ltv.xlsx')
