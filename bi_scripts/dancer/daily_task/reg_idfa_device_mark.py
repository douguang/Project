#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  元气江湖 cnwnyqjh 这个包跟之前咱们所有武娘历史IOS用户的设备号重叠情况
@software: PyCharm 
@file: reg_idfa_device_mark.py 
@time: 18/1/11 下午3:12 
"""

import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range
import datetime
import pandas as pd


def data_reduce():

    info_sql = '''
    select t1.ds,t1.account,t1.user_id,t1.device_mark,t1.appid,t2.account_l,t2.user_id_l,t2.reg_ds from (
        select regexp_replace(to_date(reg_time),'-','') as ds,account,user_id,device_mark,appid from mid_info_all where ds='20180110' and device_mark like '%-%' and appid = 'cnwnyqjh' group by ds,account,user_id,device_mark,appid 
    )t1
    left outer join(
        select account as account_l,user_id as user_id_l,device_mark,regexp_replace(to_date(reg_time),'-','') as reg_ds from mid_info_all where ds='20180110' group by account,user_id,device_mark,reg_ds
    )t2 on t1.device_mark=t2.device_mark
    where t1.user_id != t2.user_id_l and t1.device_mark != '00000000-0000-0000-0000-000000000000'
    group by t1.ds,t1.account,t1.user_id,t1.device_mark,t1.appid,t2.account_l,t2.user_id_l,t2.reg_ds
    '''
    #    where t1.device_mark != '00000000-0000-0000-0000-000000000000'
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head()
    print info_df.__len__()

    def card_evo_lines():
        for _, row in info_df.iterrows():
            now = datetime.datetime.strptime(str(row.reg_ds), '%Y%m%d')
            end = datetime.datetime.strptime(str(row.ds), '%Y%m%d')

            if now < end:
                # print [row.ds, row.server, row.reg_time,row.dau, delta]
                print [row.ds,row.account,row.user_id,row.device_mark,row.appid,row.account_l,row.user_id_l,row.reg_ds,]
                yield [row.ds,row.account,row.user_id,row.device_mark,row.appid,row.account_l,row.user_id_l,row.reg_ds,]

    result_df = pd.DataFrame(card_evo_lines(), columns=['ds', 'account', 'user_id','device_mark','appid','account_l', 'user_id_l', 'reg_ds',])
    # result_df = info_df[info_df.user_id!=info_df.user_id]
    print result_df.head()
    result_df = result_df.groupby(['ds',]).agg({
        'account': lambda g: g.nunique(),
    }).reset_index().rename(columns={'account': 'num',})

    return result_df

if __name__ == '__main__':
    #dis_kaiqigu_pay_detail('20170102')
    for platform in ['dancer_pub',]:
        settings_dev.set_env(platform)
        result = data_reduce()
        result.to_excel(r'/Users/kaiqigu/Documents/Dancer/武娘-国内-元气江湖cnwnyqjh跟之前咱们所有武娘历史IOS用户的设备号重叠情况_20180111.xlsx', index=False)
    print "end"