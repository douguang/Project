#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-23 下午4:12
@Author  : Andy 
@File    : simulator_nginxlog.py
@Software: PyCharm
Description :
'''

#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-16 上午10:58
@Author  : Andy
@File    : channel_virtual_machine.py
@Software: PyCharm
Description :
'''
import os
import gzip
import time

for filename in os.listdir(r'/home/data/dancer_pub/nginx_log'):
    date_list = ['access.log-20161110.gz',
                 'access.log-20161111.gz',
                 'access.log-20161112.gz',
                 'access.log-20161113.gz',
                 'access.log-20161114.gz',
                 'access.log-20161115.gz',
                 'access.log-20161116.gz',
                 'access.log-20161117.gz',
                 'access.log-20161118.gz',
                 'access.log-20161119.gz',
                 'access.log-20161120.gz',
                 'access.log-20161121.gz',
                 'access.log-20161122.gz',
                 'access.log-20161123.gz',
                 'access.log-20161124.gz',
                 'access.log-20161125.gz',
                 'access.log-20161126.gz',
                 'access.log-20161127.gz',
                 'access.log-20161128.gz',
                 'access.log-20161129.gz',
                 'access.log-20161130.gz',
                 'access.log-20161201.gz',
                 'access.log-20161202.gz',
                 'access.log-20161203.gz',
                 'access.log-20161204.gz',
                 'access.log-20161205.gz',
                 'access.log-20161206.gz',
                 'access.log-20161207.gz',
                 'access.log-20161208.gz',
                 'access.log-20161209.gz',
                 'access.log-20161210.gz',
                 'access.log-20161211.gz',
                 'access.log-20161212.gz',
                 'access.log-20161213.gz',]
    for date in date_list:
        log = gzip.open(date)
        acc_dict = {}
        error = open('simulator_error.txt', 'w')
        name = log.name.split('-')[1].split('.')[0]
        print name
        res = open('os_new_user_rate_%s.txt' % name, 'a+')
        for i in log:
            #print i
            try:
                if 'new_user' in i:
                    device = i.split('device_mark=')[1].split('&')[0].strip()
                    #print "vvvvvvvvvvvvvvvvvv"
                    if device == '' :
                        device = '&&'
                    ip = i.split(' - -')[0].strip()
                    #print ip
                    time = i.split('[')[1].split(']')[0].strip()
                    #time = i.split('&__ts=')[1].split('&')[0].strip()
                    #print time
                    #time = time.strftime('%Y%m%d', time.localtime(int(ts_time)))
                    #print time
                    plat = i.split('&pt_chl=')[1].split('&')[0].strip()
                    #print plat
                    if 'account' in i:
                        account = i.split('account=')[1].split('&')[0].strip()
                    else:
                        account = 'account'

                    if '&device=' in i:
                        phone = i.split('&device=')[1].split('&')[0].strip()
                    else:
                        phone = 'KKK'
                    # & osver =
                    if '&osver=' in i:
                        osver = i.split('&osver=')[1].split('&')[0].strip()
                    else:
                        osver = 'osver'
                    meth = 1
                    key = '%s\t%s\t%s\t%s\t%s\t%s\t%s' % (ip, plat, account, device, meth,phone,osver)
                    value = time
                    #print key
                    if key not in acc_dict:
                        acc_dict[key] = value
                        #print acc_dict.keys()
                elif 'server_list' in i:
                    device = i.split('device_mark=')[1].split('&')[0].strip()
                    #print "ggggggggggggg"
                    if device == '' :
                        device = '&&'
                    ip = i.split(' - -')[0].strip()
                    #print "ip",ip
                    #time = i.split('&__ts=')[1].split('&')[0].strip()
                    #print time
                    #time = time.strftime('%Y%m%d', time.localtime(int(ts_time)))
                    #print time
                    time = i.split('[')[1].split(']')[0].strip()
                    plat = i.split('&pt_chl=')[1].split('&')[0].strip()
                    #print plat
                    if 'account' in i:
                        account = i.split('account=')[1].split('&')[0].strip()
                    else:
                        account = 'account'
                    if '&device=' in i:
                        phone = i.split('&device=')[1].split('&')[0].strip()
                    else:
                        phone = 'KKK'
                    #& osver =
                    if '&osver=' in i:
                        osver = i.split('&osver=')[1].split('&')[0].strip()
                    else:
                        osver = 'osver'
                    meth = 0
                    key = '%s\t%s\t%s\t%s\t%s\t%s\t%s' % (ip, plat, account, device,meth,phone,osver)
                    #print key
                    value = time
                    if key not in acc_dict:
                        acc_dict[key] = value
            except:
                error.write('%s\n' % i)
        for d in acc_dict:
            #print d
            res.write('%s\t%s\n' % (d,acc_dict[d]))
    res.close()
    print "end"

# 完成文档合并操作
jg = open('q_new_rate.txt','w')
for filename in os.listdir(r'/home/data/dancer_pub/nginx_log'):
    if filename.startswith('os_new_user_rate'):
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
