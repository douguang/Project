#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-6-28 下午5:03
@Author  : Andy 
@File    : demo_nginx.py
@Software: PyCharm
Description :
'''


import gzip
import os
for filename in os.listdir(r'/home/data/sanguo_tl/nginx_log'):
    if filename in ('access.log-20170501.gz',
                    'access.log-20170502.gz',
                    'access.log-20170503.gz',
                    'access.log-20170504.gz',
                    'access.log-20170505.gz',
                    'access.log-20170506.gz',
                    'access.log-20170507.gz',
                    'access.log-20170508.gz',
                    'access.log-20170509.gz',
                    'access.log-20170510.gz',
                    'access.log-20170511.gz',
                    'access.log-20170512.gz',
                    'access.log-20170513.gz',
                    'access.log-20170514.gz',
                    'access.log-20170515.gz',
                    'access.log-20170516.gz',
                    'access.log-20170517.gz',
                    'access.log-20170518.gz',
                    'access.log-20170519.gz',
                    'access.log-20170520.gz',
                    'access.log-20170521.gz',
                    'access.log-20170522.gz',
                    'access.log-20170523.gz',
                    'access.log-20170524.gz',
                    'access.log-20170525.gz',
                    'access.log-20170526.gz',
                    'access.log-20170527.gz',
                    'access.log-20170528.gz',
                    'access.log-20170529.gz',
                    'access.log-20170530.gz',
                    'access.log-20170531.gz',
                    'access.log-20170601.gz',
                    'access.log-20170602.gz',
                    'access.log-20170603.gz',
                    'access.log-20170604.gz',
                    'access.log-20170605.gz',
                    'access.log-20170606.gz',
                    'access.log-20170607.gz',
                    'access.log-20170608.gz',
                    'access.log-20170609.gz'):
        print filename
        file_log = gzip.open(filename)
        res = open('pay_callback.txt','a+')
        try:
            for i in file_log:
                if 'pay-callback-kaiqigu' in i:
                        res.write(i)
        except Exception, e:
            print e
print 'guide_over'
res.close()
