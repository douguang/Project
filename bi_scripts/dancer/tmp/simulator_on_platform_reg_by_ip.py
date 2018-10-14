#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-26 下午2:08
@Author  : Andy 
@File    : simulator_on_platform_reg_by_ip.py
@Software: PyCharm
Description : 按IP统计新增
'''

import gzip
import os
import time

import pandas as pd
def data_reader(file_url):
    print file_url
    file_log = open(file_url)
    ip_list=[]
    plat_list = []
    account_list=[]
    device_list=[]
    meth_list=[]
    phone_list=[]
    time_list=[]
    osver_list=[]
    is_no_account_list=[]
    for i in file_log:
        try:
            a = i.split('\t')
            #print a
            ip = a[0]
            plat = a[1]
            account= a[2]
            device = a[3]
            #print device
            meth = a[4]
            phone = a[5]
            ver = a[6]
            ts_time = a[7]
            ts_time = ts_time[:11]
            m_list = ts_time.split('/')
            if m_list[1] == 'Dec':
                m_list[1] = '12'
            if m_list[1] == 'Nov':
                m_list[1] = '11'
            ts_time = m_list[2] + m_list[1] + m_list[0]
            #print ts_time
            #ts_time = ts_time.split('\n')[0].strip()
            #time_ds = time.strftime('%Y%m%d', time.localtime(int(ts_time)))
            #time = time.strftime('%Y%m%d', time.localtime(int(ts_time)))
            #print time_ds

            if "Redmi" in phone:
                phone = 'MI'
            if "mi" in phone:
                phone = 'MI'
            if "m1" in phone:
                phone = 'MI'
            if "m2" in phone:
                phone = 'MI'
            if "m3" in phone:
                phone = 'MI'
            if "Mi" in phone:
                phone = 'MI'
            if "MI" in phone:
                phone = 'MI'
            if "M1" in phone:
                phone = 'MI'
            if "M2" in phone:
                phone = 'MI'
            if "M3"  in phone:
                phone = 'MI'
            if "M4"  in phone:
                phone = 'MI'
            if "M5"  in phone:
                phone = 'MI'

            # if load_name =='loading':
            #     load_name=1
            # else:
            #     load_name = 0
            # is_no_account=1
            # if account =='account':
            #     is_no_account = 0
            ip_list.append(ip)
            plat_list.append(plat)
            account_list.append(account)
            device_list.append(device)
            meth_list.append(meth)
            phone_list.append(phone)
            osver_list.append(ver)
            time_list.append(ts_time)
            # is_no_account_list.append(is_no_account)

        except Exception, e:
            print e

    result_df = pd.DataFrame({'ip':ip_list,'plat': plat_list, 'account': account_list, 'device': device_list,'meth':meth_list,'phone':phone_list,'osver':osver_list,'time':time_list})
    print result_df.head(6)

    result_account_df = result_df[result_df['account'] != 'account']
    result_account_df = result_account_df[result_account_df['meth'] == '1']
    result_account_df = result_account_df.groupby(['time','plat',]).agg(
        {'ip': lambda g: g.nunique()}).reset_index()
    result_account_df = pd.DataFrame(result_account_df).reindex()
    result_account_df = result_account_df.rename(
        columns={'ip': 'account_ip_num', })
    print result_account_df.head(10)

    result_account_df.to_excel("/home/kaiqigu/桌面/武娘_模拟器用户_根据IP统计的新增.xlsx", index=False)
    #result_noaccount_df = result_df[result_df['meth'] == 0]


    #
    #print result_df.head(10)
    #final_df = result_df[['device','is_no_account','time','osver','phone','ip']]
    #final_df = pd.DataFrame(final_df).drop_duplicates()
    # final_df.groupby(['device','is_no_account','osver','phone','ip']).agg(
    #     {'time': lambda g: g.min()}).reset_index()
    #final_df = final_df[(final_df['time']>'20161109' )& (final_df['time']<'20161213' )]

    #统计每天设备的总IP
    # final_a_df = final_df.groupby(['device','time']).agg(
    #     {'ip': lambda g: g.sum()}).reset_index()
    # final_a_df = final_a_df.rename(
    #     columns={'ip': 'all_ip_num', })
    # final_a_df = pd.DataFrame(final_a_df).reindex()
    # print final_a_df.head(3)
    #
    # #计算每天的设备的IP数
    # mid_1_df = final_df[final_df['is_no_account'] == 1]
    # #mid_1_df = pd.DataFrame(mid_1_df).drop_duplicates()
    # final_1_df = mid_1_df.groupby(['device', 'time', 'osver', 'phone']).agg(
    #     {'ip': lambda g: g.nunique()}).reset_index()
    # final_1_df = final_1_df.rename(
    #     columns={'ip': 'ip_1_num', })
    # final_1_df = pd.DataFrame(final_1_df).reindex()
    # print final_1_df.head(3)
    # mid_0_df = final_df[final_df['is_no_account'] == 0]
    # #mid_0_df = pd.DataFrame(mid_0_df).drop_duplicates()
    # final_0_df = mid_0_df.groupby(['device', 'time', 'osver', 'phone']).agg(
    #     {'ip': lambda g: g.nunique()}).reset_index()
    # final_0_df = final_0_df.rename(
    #     columns={'ip': 'ip_0_num', })
    # final_0_df = pd.DataFrame(final_0_df).reindex()
    # print final_0_df.head(3)
    #
    # mid_all_1_df = final_df[final_df['is_no_account'] == 1]
    # #mid_all_1_df = pd.DataFrame(mid_all_1_df).drop_duplicates()
    # final_all_1_df = mid_all_1_df.groupby(['device', 'time',]).agg(
    #     {'ip': lambda g: g.nunique()}).reset_index()
    # final_all_1_df = final_all_1_df.rename(
    #     columns={'ip': 'ip_1_num', })
    # final_all_1_df = pd.DataFrame(final_all_1_df).reindex()
    # print final_all_1_df.head(3)
    # mid_all_0_df = final_df[final_df['is_no_account'] == 0]
    # #mid_all_0_df = pd.DataFrame(mid_all_0_df).drop_duplicates()
    # final_all_0_df = mid_all_0_df.groupby(['device', 'time', ]).agg(
    #     {'ip': lambda g: g.nunique()}).reset_index()
    # final_all_0_df = final_all_0_df.rename(
    #     columns={'ip': 'ip_0_num', })
    # final_all_0_df = pd.DataFrame(final_all_0_df).reindex()
    # print final_all_0_df.head(3)
    #
    #
    # result = final_1_df.merge(final_0_df, on=['device', 'time', 'osver', 'phone'], how='left')
    # result = result.merge(final_all_1_df, on=['device', 'time', ], how='left')
    # result = result.merge(final_all_0_df, on=['device', 'time', ], how='left')
    # result = pd.DataFrame(result).reindex()
    # result.to_excel("/home/kaiqigu/桌面/武娘_模拟器用户_统计设备4版本号.xlsx", index=False)
    # print pd.DataFrame(result).head(3)
    #

    #result.to_excel("/home/kaiqigu/桌面/武娘_模拟器用户_统计设备c版本号.xlsx", index=False)
    return ''


if __name__ == '__main__':
    file_url ='/home/kaiqigu/桌面/Q_new_rate.txt'
    data_reader(file_url)
    print "end"


