#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from utils import DateFormat

# guid_list = ['platform_access', 'get_user_server_list', 'loading',
#              'all_config', 'new_user', 'mark_user_login', 'cards.open',
#              'private_city.open', 'user.rename', 'user.guide']

path = '/Users/kaiqigu/Documents/scripts/nginx_log/'
filename_list = [
    # 'access.log_20170117'
    # ,'access.log_20170118'
    'access.log_20170203'
    # ,'access.log_20170202'
]

for filename in filename_list:
    print '{0} start!'.format(filename)
    file_log = open(path + filename, 'r')
    file_out = open(path + filename.split('.')[1], 'a+')
    file_out.write('stmp' + '\t' + str('method') + '\t' + str('account') + '\t'
                   + str('pt') + '\t' + str('device_mark') + '\n')

    for i in file_log:
        if 'method=' in i:
            if 'device_mark=' in i:
                try:
                    stmp = i.split('&')[0].split(' "')[0].split('- - [')[
                        1].split(' +')[0]
                    # uid = i.split('user_token=')[1].split('&')[0].strip()
                    method = i.split('method=')[1].split('&')[0]
                    device_mark = i.split('device_mark=')[1].split('&')[0]
                    # if method in guid_list:
                    stmp = DateFormat(stmp, '%Y-%m-%d %H:%M:%S',
                                      '%d/%b/%Y:%H:%M:%S')
                    if 'account=' in i:
                        account = i.split('account=')[1].split('&')[0]
                    else:
                        account = ''
                    if 'pt=' in i:
                        pt = i.split('pt=')[1].split('&')[0]
                    else:
                        pt = ''
                    # print stmp, method, account, pt, device_mark
                    file_out.write(str(stmp) + '\t' + str(method) + '\t' + str(
                        account) + '\t' + str(pt) + '\t' + str(device_mark) +
                                   '\n')
                except Exception as e:
                    print e

    print '{0} complete!'.format(filename)
    file_log.close()
    file_out.close()
