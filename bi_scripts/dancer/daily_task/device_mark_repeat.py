#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  设备码重合
@software: PyCharm 
@file: device_mark_repeat.py 
@time: 18/1/30 上午11:03 
"""


import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range
import datetime
import pandas as pd
import sys
reload(sys)
sys.setdefaultencoding('utf8')


def data_reduce():
    settings_dev.set_env('dancer_cgame')

    info_cgame_sql = '''
    select device_mark,account,user_id,to_date(reg_time)as reg_time  from mid_info_all where ds='20180129' group by device_mark,account,user_id,reg_time 
    '''
    print info_cgame_sql
    info_cgame_df = hql_to_df(info_cgame_sql)
    print info_cgame_df.head()
    info_cgame_df.to_excel(r'/Users/kaiqigu/Documents/Dancer/2武娘-cgame-deivcemark重合数据-info_cgame_df_20180130.xlsx', index=False)

    settings_dev.set_env('dancer_pub')
    info_pub_sql = '''
        select device_mark,account as account_pub from mid_info_all where ds='20180129' and  act_time >='2017-10-29 00:00:00' and   device_mark != 'ff:ff:ff:ff:ff:ff' and   device_mark != '02:00:00:00:00:00' and  device_mark != '00:00:00:00:00:00' and device_mark != '00000000-0000-0000-0000-000000000000' group by device_mark,account_pub
        '''
    #    where t1.device_mark != '00000000-0000-0000-0000-000000000000'
    print info_pub_sql
    info_pub_df = hql_to_df(info_pub_sql)
    print info_pub_df.head()
    info_pub_df.to_excel(r'/Users/kaiqigu/Documents/Dancer/2武娘-cgame-deivcemark重合数据-info_pub_df_20180130.xlsx', index=False)

    settings_dev.set_env('dancer_bt')
    info_bt_sql = '''
            select device_mark,account as account_bt from mid_info_all where ds='20180129' and  act_time >='2017-12-05 00:00:00' and   device_mark != 'ff:ff:ff:ff:ff:ff' and  device_mark != '02:00:00:00:00:00' and  device_mark != '00:00:00:00:00:00' and device_mark != '00000000-0000-0000-0000-000000000000' group by device_mark,account_bt
            '''
    #    where t1.device_mark != '00000000-0000-0000-0000-000000000000'
    print info_bt_sql
    info_bt_df = hql_to_df(info_bt_sql)
    print info_bt_df.head()
    info_bt_df.to_excel(r'/Users/kaiqigu/Documents/Dancer/2武娘-cgame-deivcemark重合数据-info_bt_df_20180130.xlsx', index=False)

    res_df = info_cgame_df.merge(info_pub_df, on=['device_mark', ], how='left')
    res_df = res_df.merge(info_bt_df, on=['device_mark', ], how='left')
    print res_df.head()
    res_df.to_excel(r'/Users/kaiqigu/Documents/Dancer/2武娘-cgame-deivcemark重合数据-1_20180130.xlsx', index=False)
    # print res_df.__len__()
    #
    # def card_evo_lines():
    #     for _, row in info_df.iterrows():
    #         now = datetime.datetime.strptime(str(row.reg_ds), '%Y%m%d')
    #         end = datetime.datetime.strptime(str(row.ds), '%Y%m%d')
    #
    #         if now < end:
    #             # print [row.ds, row.server, row.reg_time,row.dau, delta]
    #             print [row.ds,row.account,row.user_id,row.device_mark,row.appid,row.account_l,row.user_id_l,row.reg_ds,]
    #             yield [row.ds,row.account,row.user_id,row.device_mark,row.appid,row.account_l,row.user_id_l,row.reg_ds,]
    #
    # result_df = pd.DataFrame(card_evo_lines(), columns=['ds', 'account', 'user_id','device_mark','appid','account_l', 'user_id_l', 'reg_ds',])
    # # result_df = info_df[info_df.user_id!=info_df.user_id]
    # print result_df.head()
    result_df = res_df.groupby(['reg_time',]).agg({
        'account': lambda g: g.nunique(),
        'account_pub': lambda g: g.nunique(),
        'account_bt': lambda g: g.nunique(),
    }).reset_index().rename(columns={'account': 'account_num','account_pub': 'account_pub_num','account_bt': 'account_bt_num',})
    result_df.to_excel(r'/Users/kaiqigu/Documents/Dancer/2武娘-cgame-deivcemark重合数据-2_20180130.xlsx', index=False)

if __name__ == '__main__':
    result = data_reduce()

    print "end"