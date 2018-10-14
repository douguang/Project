#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 历史登陆天数
'''
import datetime
import pandas as pd
from utils import hql_to_df
import settings
settings.set_env('superhero_bi')

# act_sql = '''
# select ds, uid
# from raw_act
# '''
# act_df = hql_to_df(act_sql)

# print act_df


# 七酷
qiku_reg_time_sql = '''
select uid, create_time
from superhero_qiku.raw_info
where ds >= '20160501' and ds <= '20160520'
'''
qiku_reg_time_df = hql_to_df(qiku_reg_time_sql).drop_duplicates()
qiku_reg_time_df['act_days'] = qiku_reg_time_df.create_time.map(lambda d: (datetime.datetime.today() - datetime.datetime.strptime(d[:10], '%Y-%m-%d')).days)
qiku_reg_time_df.to_excel('/tmp/superhero_login_days_qiku.xlsx')

# # 国内和pub
# reg_sql = '''
# select uid, create_time
# from raw_info
# where ds >= '20160501' and ds <= '20160520'
# '''
# reg_df = hql_to_df(reg_sql).drop_duplicates()

# act_days_df = pd.read_excel(open('/Users/huwenchao/kaiqigu/document/20160524/superhero_login_days.xlsx', 'rb'), sheetname='Sheet1')
# mid_df = act_days_df.merge(reg_df)

# mid_df['no_record_days'] = mid_df.create_time.map(lambda d: 0 if d >= '2014-10-17' else (datetime.datetime(2014, 10, 17) - datetime.datetime.strptime(d, '%Y-%m-%d')).days)
# mid_df['login_days_with_no_record'] = mid_df['no_record_days'] + mid_df['ds']
# mid_df.to_excel('/tmp/superhero_login_days.xlsx')
