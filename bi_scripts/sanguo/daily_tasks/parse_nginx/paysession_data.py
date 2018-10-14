#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-7-6 下午6:31
@Author  : Andy 
@File    : paysession_data.py
@Software: PyCharm
Description :

96.126.115.253 - - [10/Jun/2017:04:13:43 +0800] "POST /taiwan/pay_callback_payssion/ HTTP/1.1" 200 17 app_name=ROBOT+%E0%B8%AA%E0%B8%B2%E0%B8%A1%E0%B8%81%E0%B9%8A%E0%B8%81&pm_id=telcovoucher_vn&transaction_id=H609389093026497&track_id=kthjjws537271&sub_track_id=&amount=200000.00&paid=200000.00&net=130000.00&fee=70000.00&currency=VND&description=order+kthjjws537271&payer_email=jinbao.zhao%40kaiqigu.com&state=completed&notify_sig=b12bb34b8406f05d0c78fd59a2f55c8c "-" "CodaHTTPClient/1.0" "-"

'''

import pandas as pd
import json
import sys
from urllib import unquote
reload(sys)
sys.setdefaultencoding('utf-8')

def reduce_nginx(line):
    if 'pay_callback_payssion' in line:
        print "start"
        print i
        data_dic={'ip':'','time':'','get_post':'','api_type':'','app_name':'','m_id':'','transaction_id':'','track_id':'','sub_track_id':'','amount':'','paid':'','net':'','fee':'','currency':'','description':'','payer_email':'','payer_name':'','state':'','notify_sig':'',}
        data_list = ['ip','time','get_post','api_type','app_name','m_id','transaction_id','track_id','sub_track_id','amount','paid','net','fee','currency','description','payer_email','payer_name','state','notify_sig',]
        data = line.split('POST /')[1].split('"')[1].strip()
        print '---data'
        print data
        if 'real_amount=' in data:
            print "************"
            print data
        if data != '' and data.find('http://') == -1 and data.find('\\x') == -1 and data.find('CONNECT') == -1:
            data_dic['ip'] = line.split(' -')[0].strip()
            data_dic['time'] = line.split('[')[1].split(' +')[0].strip()
            data_dic['get_post'] = line.split('"')[1].split(' /')[0].strip()
            data_dic['api_type'] = line.split('"')[1].split(' /')[1].split('/ HTTP/1.1')[0].strip()

            if 'account_id=' in data: data_dic['account_id'] = data.split('account_id=')[1].split('&')[0].strip()
            if 'order_no=' in data: data_dic['order_no'] = data.split('order_no=')[1].split('&')[0].strip()
            if 'ext=' in data: data_dic['ext'] = data.split('ext=')[1].split('&')[0].strip()
            if 'ext=' in data:
                a = data.split('ext=')[1].split('&')[0].strip()
                data_dic['user_id'] = a.split('-', 5 )[0]
                data_dic['server_id'] = a.split('-', 5 )[1]
                data_dic['item'] = a.split('-', 5 )[2]
                data_dic['order_time'] = a.split('-', 5 )[3]
                data_dic['pl'] = a.split('-', 5 )[4]
                # print data_dic
            if 'currency=' in data: data_dic['currency'] = data.split('currency=')[1].split('&')[0].strip()
            if 'amount=' in data: data_dic['amount'] = data.split('amount=')[1].split('&')[0].strip()
            if 'pay_tp=' in data: data_dic['pay_tp'] = data.split('pay_tp=')[1].split('&')[0].strip()
            if 'signature=' in data: data_dic['signature'] = data.split('signature=')[1].split('&')[0].strip()
            if 'product_name=' in data: data_dic['product_name'] = data.split('product_name=')[1].split('&')[0].strip()
            if 'virtualCurrencyAmount=' in data: data_dic['virtualCurrencyAmount'] = data.split('virtualCurrencyAmount=')[1].split('&')[0].strip()
            if 'real_amount=' in data:
                print 'real_amount=',data
                data_dic['real_amount'] = data.split('real_amount=')[1].split('&')[0].strip()
                data_dic['amount'] = data.split('real_amount=')[1].split('amount=')[1].split('&')[0].strip()
            print data_dic
            print  [data_dic[key] for key in data_list]
            return [data_dic[key] for key in data_list]

if __name__ == '__main__':
    log = open('/home/kaiqigu/桌面/pay_callback.txt')
    result_list=[]
    for i in log:
        print '-----------'
        print i
        a = reduce_nginx(i)
        result_list.append(a)
    pd.DataFrame(result_list).to_excel('/home/kaiqigu/桌面/机甲无双-多语言-Payssion支付_20170706.xlsx', index=False)
    print "end"