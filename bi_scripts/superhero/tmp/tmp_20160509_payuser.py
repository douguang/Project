#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 越南 新付费玩家充值金额占比
'''
import pandas as pd
from pandas import DataFrame
import datetime

def ds_add(date, delta, date_format='%Y%m%d'):
    return datetime.datetime.strftime(
        datetime.datetime.strptime(
            date, date_format) + datetime.timedelta(delta), date_format)

def reg_7_user(date):
    r1 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(date),names=['uid','stamp','a1'])
    r2 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-1)),names=['uid','stamp','a1'])
    r3 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-2)),names=['uid','stamp','a1'])
    r4 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-3)),names=['uid','stamp','a1'])
    r5 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-4)),names=['uid','stamp','a1'])
    r6 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-5)),names=['uid','stamp','a1'])
    r7 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-6)),names=['uid','stamp','a1'])
    r1['ds'] = date
    r2['ds'] = ds_add(date,-1)
    r3['ds'] = ds_add(date,-2)
    r4['ds'] = ds_add(date,-3)
    r5['ds'] = ds_add(date,-4)
    r6['ds'] = ds_add(date,-5)
    r7['ds'] = ds_add(date,-6)
    rr =  pd.concat([r1,r2,r3,r4,r5,r6,r7])
    columns = ['uid','ds']
    rr = rr[columns]
    return rr

def reg_15_user(date):
    r1 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(date),names=['uid','stamp','a1'])
    r2 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-1)),names=['uid','stamp','a1'])
    r3 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-2)),names=['uid','stamp','a1'])
    r4 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-3)),names=['uid','stamp','a1'])
    r5 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-4)),names=['uid','stamp','a1'])
    r6 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-5)),names=['uid','stamp','a1'])
    r7 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-6)),names=['uid','stamp','a1'])
    r8 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-7)),names=['uid','stamp','a1'])
    r9 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-8)),names=['uid','stamp','a1'])
    r10 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-9)),names=['uid','stamp','a1'])
    r11 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-10)),names=['uid','stamp','a1'])
    r12 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-11)),names=['uid','stamp','a1'])
    r13 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-12)),names=['uid','stamp','a1'])
    r14 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-13)),names=['uid','stamp','a1'])
    r15 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-14)),names=['uid','stamp','a1'])
    r1['ds'] = date
    r2['ds'] = ds_add(date,-1)
    r3['ds'] = ds_add(date,-2)
    r4['ds'] = ds_add(date,-3)
    r5['ds'] = ds_add(date,-4)
    r6['ds'] = ds_add(date,-5)
    r7['ds'] = ds_add(date,-6)
    r8['ds'] = ds_add(date,-7)
    r9['ds'] = ds_add(date,-8)
    r10['ds'] = ds_add(date,-9)
    r11['ds'] = ds_add(date,-10)
    r12['ds'] = ds_add(date,-11)
    r13['ds'] = ds_add(date,-12)
    r14['ds'] = ds_add(date,-13)
    r15['ds'] = ds_add(date,-14)

    rr =  pd.concat([r1,r2,r3,r4,r5,r6,r7,r8,r9,r10,r11,r12,r13,r14,r15])
    columns = ['uid','ds']
    rr = rr[columns]
    return rr

def reg_30_user(date):
    r1 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(date),names=['uid','stamp','a1'])
    r2 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-1)),names=['uid','stamp','a1'])
    r3 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-2)),names=['uid','stamp','a1'])
    r4 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-3)),names=['uid','stamp','a1'])
    r5 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-4)),names=['uid','stamp','a1'])
    r6 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-5)),names=['uid','stamp','a1'])
    r7 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-6)),names=['uid','stamp','a1'])
    r8 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-7)),names=['uid','stamp','a1'])
    r9 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-8)),names=['uid','stamp','a1'])
    r10 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-9)),names=['uid','stamp','a1'])
    r11 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-10)),names=['uid','stamp','a1'])
    r12 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-11)),names=['uid','stamp','a1'])
    r13 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-12)),names=['uid','stamp','a1'])
    r14 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-13)),names=['uid','stamp','a1'])
    r15 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-14)),names=['uid','stamp','a1'])
    r16 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-15)),names=['uid','stamp','a1'])
    r17 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-16)),names=['uid','stamp','a1'])
    r18 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-17)),names=['uid','stamp','a1'])
    r19 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-18)),names=['uid','stamp','a1'])
    r20 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-19)),names=['uid','stamp','a1'])
    r21 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-20)),names=['uid','stamp','a1'])
    r22 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-21)),names=['uid','stamp','a1'])
    r23 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-22)),names=['uid','stamp','a1'])
    r24 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-23)),names=['uid','stamp','a1'])
    r25 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-24)),names=['uid','stamp','a1'])
    r26 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-25)),names=['uid','stamp','a1'])
    r27 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-26)),names=['uid','stamp','a1'])
    r28 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-27)),names=['uid','stamp','a1'])
    r29 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-28)),names=['uid','stamp','a1'])
    r30 =  pd.read_table('/Users/kaiqigu/Downloads/super_file/reg_{0}'.format(ds_add(date,-29)),names=['uid','stamp','a1'])
    r1['ds'] = date
    r2['ds'] = ds_add(date,-1)
    r3['ds'] = ds_add(date,-2)
    r4['ds'] = ds_add(date,-3)
    r5['ds'] = ds_add(date,-4)
    r6['ds'] = ds_add(date,-5)
    r7['ds'] = ds_add(date,-6)
    r8['ds'] = ds_add(date,-7)
    r9['ds'] = ds_add(date,-8)
    r10['ds'] = ds_add(date,-9)
    r11['ds'] = ds_add(date,-10)
    r12['ds'] = ds_add(date,-11)
    r13['ds'] = ds_add(date,-12)
    r14['ds'] = ds_add(date,-13)
    r15['ds'] = ds_add(date,-14)
    r16['ds'] = ds_add(date,-15)
    r17['ds'] = ds_add(date,-16)
    r18['ds'] = ds_add(date,-17)
    r19['ds'] = ds_add(date,-18)
    r20['ds'] = ds_add(date,-19)
    r21['ds'] = ds_add(date,-20)
    r22['ds'] = ds_add(date,-21)
    r23['ds'] = ds_add(date,-22)
    r24['ds'] = ds_add(date,-23)
    r25['ds'] = ds_add(date,-24)
    r26['ds'] = ds_add(date,-25)
    r27['ds'] = ds_add(date,-26)
    r28['ds'] = ds_add(date,-27)
    r29['ds'] = ds_add(date,-28)
    r30['ds'] = ds_add(date,-29)

    rr =  pd.concat([r1,r2,r3,r4,r5,r6,r7,r8,r9,r10,r11,r12,r13,r14,r15,r16,r17,r18,r19,r20,r21,r22,r23,r24,r25,r26,r27,r28,r29,r30])
    columns = ['uid','ds']
    rr = rr[columns]
    return rr

if __name__ == '__main__':
    date = '20160217'
    sum_data = {'sum_7':[0],'sum_15':[0],'sum_30':[0],'sum_all':[0],'ds':[0]}
    sum_data_f = DataFrame(sum_data)
    for i in range(74):
        if  date <='20160430':
            print date

            tt =  pd.read_table('/Users/kaiqigu/Downloads/super_file/paylog_{0}'.format(date),names=['order_id','admin','gift_coin','level','old_coin','order_coin','order_money','order_rmb','order_time','platform_2','product_id','raw_data','reason','scheme_id','user_id','m','n'])
            tt['ds'] = date
            columns = ['user_id','order_rmb','ds']
            tt = tt[columns]
            r1 = reg_7_user(date)
            r2 = reg_15_user(date)
            r3 = reg_30_user(date)
            tt['is_7d_user'] = tt['user_id'].isin(r1.uid.values)
            tt['is_15d_user'] = tt['user_id'].isin(r2.uid.values)
            tt['is_30d_user'] = tt['user_id'].isin(r3.uid.values)

            mm = tt['order_rmb'].sum()
            d7_user = tt.groupby('is_7d_user').sum().reset_index()
            d15_user = tt.groupby('is_15d_user').sum().reset_index()
            d30_user = tt.groupby('is_30d_user').sum().reset_index()

            # print d7_user
            # print d15_user
            sum_7 = 0
            if d7_user['is_7d_user'][0] == True:
                sum_7 = d7_user['order_rmb'][0]
            else:
                sum_7 = d7_user['order_rmb'][1]

            sum_15 = 0
            if d15_user['is_15d_user'][0] == True:
                sum_15 = d15_user['order_rmb'][0]
            else:
                sum_15 = d15_user['order_rmb'][1]

            sum_30 = 0
            if d30_user['is_30d_user'][0] == True:
                sum_30 = d30_user['order_rmb'][0]
            else:
                sum_30 = d30_user['order_rmb'][1]

            # print sum_7
            # print sum_15
            # print sum_30
            data = {'sum_7':[sum_7],'sum_15':[sum_15],'sum_30':[sum_30],'sum_all':[mm],'ds':[date]}
            frame = DataFrame(data)
            date = ds_add(date,1)
            # print frame
        sum_data_f =  pd.concat([sum_data_f,frame])
    columns = ['ds','sum_7','sum_15','sum_30','sum_all']
    sum_data_f = sum_data_f[columns]
    print sum_data_f
    sum_data_f.to_excel('/Users/kaiqigu/Downloads/Excel/mm.xlsx')



