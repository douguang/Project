#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from utils import hql_to_df
import pandas as pd
import settings_dev
def user_guide():
    settings_dev.set_env('dancer_tw')
    country_df = pd.read_excel(r'E:\bi_scripts\dancer\tmp\user_guide.xlsx')
    print country_df
    sql = '''
    select user_id,min(ds) as reg_date
    from mid_actionlog
    where ds >= '20160907'
    group by user_id
    '''
    reg_df = hql_to_df(sql)
    print reg_df
    user_df = country_df.merge(reg_df,on='user_id',how='left')
    print user_df
    user_df.to_excel(r'E:\My_Data_Library\dancer\tmp_20160926_ip_reg_ltv.xlsx', index=False)
if __name__ == '__main__':
    user_guide()