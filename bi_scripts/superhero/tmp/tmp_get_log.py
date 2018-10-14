#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from utils import DateFormat
from utils import date_range

path = '/Users/kaiqigu/Documents/scripts/nginx_log/'

# filename_list = [
#     'genesis_crash.log-20170201',
#     'genesis_crash.log-20170202',
#     'genesis_crash.log-20170203',
#     'genesis_crash.log-20170204',
#     'genesis_crash.log-20170118',
#     'genesis_crash.log-20170119'
# ]
filename_list = []
file_str = 'genesis_crash.log-'
for date in date_range('20170120', '20170204'):
    filename_list.append(file_str + date)

for filename in filename_list:
    print '{0} start !'.format(filename)
    file_log = open(path + filename, 'r')
    file_out = open(path + filename.split('.')[1], 'a+')
    file_out.write(str('stmp') + '\t' + str('newstep') + '\t' + str(
        'user_name') + '\t' + str('plat') + '\t' + str('platform') + '\t' +
                   str('device_mark') + '\n')
    for i in file_log:
        if 'newstep=' in i:
            try:
                stmp = i.split('&')[0].split(' "')[0].split('- - [')[1].split(
                    ' +')[0]
                stmp = DateFormat(stmp, '%Y-%m-%d %H:%M:%S',
                                  '%d/%b/%Y:%H:%M:%S')
                newstep = i.split('newstep=')[1].split('&')[0]
                user_name = i.split('user_name=')[1].split('&')[0]
                if user_name != 'nil':
                    plat = user_name.split('_')[0]
                else:
                    plat = ''
                if 'platform_channel=' in i:
                    platform = i.split('platform_channel=')[1].split('&')[0]
                else:
                    platform = ''
                device_mark = i.split('device_mark=')[1].split('&')[0]
                file_out.write(str(stmp) + '\t' + str(newstep) + '\t' + str(
                    user_name) + '\t' + str(plat) + '\t' + str(platform) + '\t'
                               + str(device_mark) + '\n')
            except Exception, e:
                print e
                print i
    print '{0} complete!'.format(filename)
    file_log.close()
    file_out.close()
