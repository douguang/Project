#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from utils import DateFormat
path = '/Users/kaiqigu/Documents/scripts/nginx_log/'
# for filename in ['access.log_20170112']:
# for filename in ['access.log_20170117', 'access.log_20170118']:
for filename in ['access.log_20170207']:
    # print filename.split('.')[1]
    file_log = open(path + filename, 'r')
    file_out = open(path + filename.split('.')[1], 'a+')
    file_out.write('stmp' + '\t' + str('uid') + '\t' + str('plat') + '\t' +
                   str('method') + '\n')

    for i in file_log:
        if 'user_token=' in i:
            try:
                stmp = i.split('&')[0].split(' "')[0].split('- - [')[1].split(
                    ' +')[0]
                uid = i.split('user_token=')[1].split('&')[0].strip()
                method = i.split('method=')[1].split('&')[0]
                if uid != '':
                    stmp = DateFormat(stmp, '%Y-%m-%d %H:%M:%S',
                                      '%d/%b/%Y:%H:%M:%S')
                    plat = uid[0:1]
                    print stmp, uid, plat, method
                    file_out.write(str(stmp) + '\t' + str(uid) + '\t' + str(
                        plat) + '\t' + str(method) + '\n')
            except Exception as e:
                print e

    file_log.close()
    file_out.close()
