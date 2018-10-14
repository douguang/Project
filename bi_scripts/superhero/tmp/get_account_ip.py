#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 提取IP
Time        : 2017.03.21
'''
# from utils import DateFormat
from utils import date_range

path = '/Users/kaiqigu/Documents/scripts/nginx_log/'

# 查询filename的第二种方法
filename_list = []
file_str = 'access.log-'
for date in date_range('20170112', '20170421'):
    filename_list.append(file_str + date)

try:
    file_out = open(path + 'log_account', 'a+')
    file_out.write('\t'.join(map(str, ['account', 'ip'])) + '\n')

    for filename in filename_list:
        print '{0} start !'.format(filename)
        try:
            file_log = open(path + filename, 'r')
            for i in file_log:
                if 'account=' in i:
                    try:
                        ip = i.split(' - - ')[0]
                        account = i.split('account=')[1].split('&')[0]
                        if (account != '') and (len(account) <= 50) and (ip != ''):
                            file_out.write(
                                '\t'.join(map(str, [account, ip])) + '\n')
                    except Exception, e:
                        print e
            print '{0} complete!'.format(filename)
        finally:
            file_log.close()
finally:
    file_out.close()
    # for filename in filename_list:
    #     if filename not in os.listdir(path):
    #         print filename
