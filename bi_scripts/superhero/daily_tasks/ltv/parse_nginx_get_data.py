#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: parse_nginx_get_data.py 
@time: 17/12/26 下午4:26 
"""

import os
import gzip
import time

for filename in os.listdir(r'/home/data/superhero/nginx_log'):
    date_list = ['access.log_20170401',
                 'access.log_20170402',
                 'access.log_20170403',
                 'access.log_20170404',
                 'access.log_20170405',
                 'access.log_20170406',
                 'access.log_20170407',
                 'access.log_20170408',
                 'access.log_20170409',
                 'access.log_20170410',
                 'access.log_20170411',
                 'access.log_20170412',
                 'access.log_20170413',
                 'access.log_20170414',
                 'access.log_20170415',
                 'access.log_20170416',
                 'access.log_20170417',
                 'access.log_20170418',
                 'access.log_20170419',
                 'access.log_20170420',
                 'access.log_20170421',
                 'access.log_20170422',
                 'access.log_20170423',
                 'access.log_20170424',
                 'access.log_20170425',
                 'access.log_20170426',
                 'access.log_20170427',
                 'access.log_20170428',
                 'access.log_20170429',
                 'access.log_20170430',
                 'access.log_20170501',
                 'access.log_20170502',
                 'access.log_20170503',
                 'access.log_20170504',
                 'access.log_20170505',
                 'access.log_20170506',
                 'access.log_20170507',
                 'access.log_20170508',
                 'access.log_20170509',
                 'access.log_20170510',
                 'access.log_20170511',
                 'access.log_20170512',
                 'access.log_20170513',
                 'access.log_20170514',
                 'access.log_20170515',
                 'access.log_20170516',
                 'access.log_20170517',
                 'access.log_20170518',
                 'access.log_20170519',
                 'access.log_20170520',
                 'access.log_20170521',
                 'access.log_20170524',
                 'access.log_20170525',
                 'access.log_20170526',
                 'access.log_20170527',
                 'access.log_20170528',
                 'access.log_20170529',
                 'access.log_20170530',
                 'access.log_20170531',
                 'access.log_20170601',
                 'access.log_20170602',
                 'access.log_20170603',
                 'access.log_20170604',
                 'access.log_20170605',
                 'access.log_20170605',
                 'access.log_20170606',
                 'access.log_20170607',
                 'access.log_20170608',
                 'access.log_20170609',
                 'access.log_20170610',
                 'access.log_20170611',
                 'access.log_20170612',
                 'access.log_20170613',
                 'access.log_20170614',
                 'access.log_20170615',
                 'access.log_20170616',
                 'access.log_20170617',
                 'access.log_20170618',
                 'access.log_20170619',
                 'access.log_20170620',
                 'access.log_20170621',
                 'access.log_20170622',
                 'access.log_20170623',
                 'access.log_20170624',
                 'access.log_20170625',
                 'access.log_20170626',
                 'access.log_20170627',
                 'access.log_20170628',
                 'access.log_20170629',
                 'access.log_20170630',]
    for date in date_list:
        log = open(date)
        acc_dict = {}
        error = open('simulator_error.txt', 'w')
        name = log.name.split('_')[1].strip()
        print name
        res = open('os_new_user_rate_%s.txt' % name, 'w')
        for i in log:
            # print i
            try:
                if 'method=new_user' in i:
                    # print '-------------'
                    account = i.split('account=')[1].split('&')[0].strip()
                    # print account
                    # print '-1-'
                    ip = i.split(' - -')[0].strip()
                    # print ip
                    # print '-2-'
                    time = i.split('[')[1].split(']')[0].strip()
                    # print time
                    # print '-3-'
                    plat = i.split('&platform_channel=')[1].split('&')[0].strip()
                    # print plat

                    key = '%s\t%s\t%s\t%s' % (ip, plat, account, time)
                    value = time
                    # print key
                    if key not in acc_dict:
                        acc_dict[key] = value
            except:
                error.write('%s\n' % i)
        for d in acc_dict:
            #print d
            res.write('%s\t%s\n' % (d,acc_dict[d]))
    break
res.close()
print "end"

# 完成文档合并操作
jg = open('q_new_rate.txt','w')
for filename in os.listdir(r'/home/data/superhero/nginx_log'):
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