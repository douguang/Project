#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-19 上午11:07
@Author  : Andy 
@File    : simulator_time_switch.py
@Software: PyCharm
Description :  模拟器转换率的时间转换————将txt转为ds
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
    for i in file_log:
        try:
            a = i.split('\t')
            #print a
            ip = a[0]
            plat = a[1]
            account= a[2]
            device = a[3]
            meth = a[4]
            phone = a[5]
            ver = a[6]
            ts_time = a[7]
            #ts_time = ts_time.split('\n')[0].strip()
            time_ds = time.strftime('%Y%m%d', time.localtime(int(ts_time)))
            #time = time.strftime('%Y%m%d', time.localtime(int(ts_time)))
            #print time_ds

            # if load_name =='loading':
            #     load_name=1
            # else:
            #     load_name = 0

            ip_list.append(ip)
            plat_list.append(plat)
            account_list.append(account)
            device_list.append(device)
            meth_list.append(meth)
            phone_list.append(phone)
            osver_list.append(ver)
            time_list.append(time_ds)

        except Exception, e:
            print e

    result_df = pd.DataFrame({'ip':ip_list,'plat': plat_list, 'account': account_list, 'device': device_list,'meth':meth_list,'phone':phone_list,'osver':osver_list,'time':time_list})

    result_df.to_excel("/home/kaiqigu/桌面/武娘_模拟器用户_转时间.xlsx", index=False)
    print result_df.head(10)
    return ''


if __name__ == '__main__':
    file_url ='/home/kaiqigu/桌面/f_new_rate.txt'
    data_reader(file_url)
    print "end"
