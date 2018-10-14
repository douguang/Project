#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-6-28 下午5:22
@Author  : Andy 
@File    : reduce_nginx.py
@Software: PyCharm
Description :  获取

118.193.27.50 - - [07/Jun/2017:02:03:54 +0700] "POST /gloryroad_th/pay-callback-kaiqigu/ HTTP/1.1" promo_code=&order_no=kthjjws528223&virtualCurrencyAmount=71&signature=de2d41c5fac463040778b7689485aa1b&real_amount=17857&amount=22000.0&currency=VND&ext=th613827729-th61-8-1496746926-thjjws&pay_tp=4&sign_type=MD5&product_name=80+KC&account_id=10509192 200 7 "-" "Python-urllib/2.7" "-" 10.10.5.34:5400 0.147 0.215

'''

import pandas as pd
import json
import sys
from urllib import unquote
reload(sys)
sys.setdefaultencoding('utf-8')

def reduce_nginx(line):
    data_dic={'ip':'','time':'','get_post':'','api_type':'','account_id':'','order_no':'','ext':'','currency':'','amount':'','pay_tp':'','signature':'','product_name':'','virtualCurrencyAmount':'','real_amount':'','user_id':'','server_id':'','item':'','order_time':'','pl':'',}
    data_list = ['ip','time','get_post','api_type','account_id','order_no','ext','currency','amount','pay_tp','signature','product_name','virtualCurrencyAmount','real_amount','user_id','server_id','item','order_time','pl',]
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
    pd.DataFrame(result_list).to_excel('/home/kaiqigu/桌面/机甲无双-泰国-Nginx_20170628.xlsx', index=False)
    print "end"