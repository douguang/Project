#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-7-6 下午4:21
@Author  : Andy 
@File    : sdk_nginx_reduce.py
@Software: PyCharm
Description :
'''

import gzip
import os
for filename in os.listdir(r'/home/data/sanguo_tl/sdk_tw_nginx_log'):
    if filename in ('app.tw.hi365.com.access.log_20170524',
                    'app.tw.hi365.com.access.log_20170525',
                    'app.tw.hi365.com.access.log_20170526',
                    'app.tw.hi365.com.access.log_20170527',
                    'app.tw.hi365.com.access.log_20170528',
                    'app.tw.hi365.com.access.log_20170529',
                    'app.tw.hi365.com.access.log_20170530',
                    'app.tw.hi365.com.access.log_20170531',
                    'app.tw.hi365.com.access.log_20170601',
                    'app.tw.hi365.com.access.log_20170602',
                    'app.tw.hi365.com.access.log_20170603',
                    'app.tw.hi365.com.access.log_20170604',
                    'app.tw.hi365.com.access.log_20170605',
                    'app.tw.hi365.com.access.log_20170606',
                    'app.tw.hi365.com.access.log_20170607',
                    'app.tw.hi365.com.access.log_20170608',
                    'app.tw.hi365.com.access.log_20170609',
                    'app.tw.hi365.com.access.log_20170610',
                    'app.tw.hi365.com.access.log_20170611',
                    'app.tw.hi365.com.access.log_20170612',
                    'app.tw.hi365.com.access.log_20170613',
                    'app.tw.hi365.com.access.log_20170614',
                    'app.tw.hi365.com.access.log_20170615',
                    'app.tw.hi365.com.access.log_20170616',
                    'app.tw.hi365.com.access.log_20170617',
                    'app.tw.hi365.com.access.log_20170618',
                    'app.tw.hi365.com.access.log_20170619',
                    'app.tw.hi365.com.access.log_20170620',
                    'app.tw.hi365.com.access.log_20170621',
                    'app.tw.hi365.com.access.log_20170622',
                    'app.tw.hi365.com.access.log_20170623',
                    'app.tw.hi365.com.access.log_20170624',
                    'app.tw.hi365.com.access.log_20170625',
                    'app.tw.hi365.com.access.log_20170626',
                    'app.tw.hi365.com.access.log_20170627',
                    'app.tw.hi365.com.access.log_20170628',
                    'app.tw.hi365.com.access.log_20170629',
                    'app.tw.hi365.com.access.log_20170630',
                    'app.tw.hi365.com.access.log_20170701',
                    'app.tw.hi365.com.access.log_20170702',
                    'app.tw.hi365.com.access.log_20170703',
                    'app.tw.hi365.com.access.log_20170704',
                    'app.tw.hi365.com.access.log_20170705'):
        print filename
        file_log = open(filename)
        res = open('pay_callback.txt','a+')
        try:
            for i in file_log:
                if 'payssion' in i:
                        res.write(i)
        except Exception, e:
            print e
print 'guide_over'
res.close()
