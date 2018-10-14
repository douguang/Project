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

# filename_list = [
#     'access.log-20170313'
#     ,'access.log-20170314'
#     ,'access.log-20170315'
#     ,'access.log-20170316'
#     ,'access.log-20170317'
#     ,'access.log-20170318'
#     ,'access.log-20170319'
#     ,'access.log-20170320'
#     ,'access.log-20170321'
# ]
# 查询filename的第二种方法
filename_list = []
file_str = 'access.log-'
for date in date_range('20170112', '20170321'):
    filename_list.append(file_str + date)
file_out = open(path + 'log', 'a+')
file_out.write('\t'.join(map(str, ['uid', 'ip'])) + '\n')

for filename in filename_list:
    print '{0} start !'.format(filename)
    file_log = open(path + filename, 'r')
    # file_out = open(path + filename.split('.')[1], 'a+')
    # file_out.write('\t'.join(map(str, ['uid', 'ip'])) + '\n')
    for i in file_log:
        if 'user_token=' in i:
            try:
                ip = i.split(' - - ')[0]
                uid = i.split('user_token=')[1].split('&')[0]
                if (uid != '') and (len(uid) <= 11) and (ip != ''):
                    file_out.write('\t'.join(map(str, [uid, ip])) + '\n')
            except Exception, e:
                print e
    print '{0} complete!'.format(filename)
    file_log.close()

file_out.close()
