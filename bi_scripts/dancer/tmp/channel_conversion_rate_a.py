#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-7 上午11:24
@Author  : Andy 
@File    : channel_conversion_rate_a.py
@Software: PyCharm
Description :  渠道转换率  从log日志中获取数据
'''

import gzip
import os
import time

for filename in os.listdir(r'/home/data/dancer_pub/nginx_log'):
    date_list = ['access.log-20161111.',
                 'access.log-20161112.',
                 'access.log-20161113.',
                 'access.log-20161114.',
                 'access.log-20161115.',
                 'access.log-20161116.',
                 'access.log-20161117.',
                 'access.log-20161118.',
                 'access.log-20161119.',
                 'access.log-20161120.',
                 'access.log-20161121.',
                 'access.log-20161122.',
                 'access.log-20161123.',
                 'access.log-20161124.',
                 'access.log-20161125.',
                 'access.log-20161126.',
                 'access.log-20161127.',
                 'access.log-20161128.',
                 'access.log-20161129.',
                 'access.log-20161130.',
                 'access.log-20161201.',
                 'access.log-20161202.',
                 'access.log-20161203.',
                 'access.log-20161204.',
                 'access.log-20161205.',
                 'access.log-20161206.',
                 'access.log-20161207.',]
    for date in date_list:
        final_dict = {}
        if filename.startswith(date):
            file_log = gzip.open(filename)
            name = filename.split('-')[1].split('.')[0]
            print name
            res = open('channel_conversion_rate_%s.txt' % name, 'a+')
            for i in file_log:
                try:
                    device = i.split('&device_mark=')[1].split('&')[0].strip()
                    #print device
                    # if device != '' and device != '02:00:00:00:00:00':
                    plat = i.split('&pt_chl=')[1].split('&')[0].strip()
                    #print plat
                    ts_time = i.split('&__ts=')[1].split('&')[0].strip()
                    #print ts_time
                    ts_ds = time.strftime('%Y%m%d', time.localtime(int(ts_time)))
                    #print ts_ds
                    # if (device != '') and (device !=im '02:00:00:00:00:00'):
                    # ip = i.split(' - -')[0].strip()
                    #print ip
                    # if '?method=get_user_server_list' in i:
                    #     if 'account' in i:
                    #         itype = '3'
                    #         account = i.split('account')[1].split('&')[0].strip()
                    #     elif 'account' not in i:
                    #         itype = '1'
                    #         account = ''
                    # elif '?method=loading' in i:
                    #     itype = '2'
                    #     if 'account' in i:
                    #         account = i.split('account')[1].split('&')[0].strip()
                    #     elif 'account' not in i:
                    #         account = ''
                    #print itype
                    #print account
                    #key = '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % (device, plat, ts_time, ts_ds,ip,itype,account)
                    key = '%s\t%s' % (device,plat)
                    value = ts_ds
                    key_value = key+'\t'+value
                    if key in final_dict:
                        if value < final_dict[key]:
                            final_dict[key] = value
                    else:
                        final_dict[key] = value
                except Exception, e:
                    print e

        for d in final_dict:
            res.write('%s\t%s\n' % (d,final_dict[d]))

res.close()
print "end"
# 完成文档合并操作
jg = open('all_channel_conversion_rate.txt','w')
for filename in os.listdir(r'/home/data/dancer_pub/nginx_log'):
    if filename.startswith('channel_conversion_rate'):
        print filename
        file_log = open(filename)
        for i in file_log:
            try:
                jg.write(i)
            except Exception, e:
                print e
jg.close()
print 'over'
exit()

