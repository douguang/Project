#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from utils import sql_to_df
import settings_dev

if __name__ == '__main__':
    settings_dev.set_env('superhero_bi')
    sql = '''
    select * from dis_keep_rate where ds >='20170501'
    '''
    df = sql_to_df(sql, 'superhero_pub')
    df.to_excel('/Users/kaiqigu/Documents/Excel/dis_keep_rate.xlsx')

