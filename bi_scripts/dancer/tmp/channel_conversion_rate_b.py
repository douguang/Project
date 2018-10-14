#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-7 上午11:30
@Author  : Andy 
@File    : channel_conversion_rate_b.py
@Software: PyCharm
Description :转化率第二步，判断用户进展到第N步，需要在27或本地执行
'''
import time
import time


log = open('/home/kaiqigu/桌面/all_channel_conversion_rate.txt')
#log = open('/home/kaiqigu/桌面/channel_conversion_rate_20161115.txt')
result_dic={}
for i in log:
    #print i
    try:
        i = i.replace('\t','$',1)
        i = i.replace('\t', '&', 2)
        #print i
        device = i.split('$')[0].strip()
        plat = i.split('$')[1].split('&')[0].strip()
        ds = i.split('$')[1].split('&')[1].strip()
        #print 'device',device
        #print 'plat',plat
        #print 'ds',ds
        key = device+'\t'+plat
        value = ds
        if key in result_dic.keys():
            if value < result_dic[key]:
                result_dic[key]=value
        else:
            result_dic[key]=value
    except Exception, e:
        print e
log.close()
res = open('/home/kaiqigu/桌面/all_channel_conversion_rate_去重.txt','w')
for d in result_dic:
    res.write('%s\t%s\n' % (d,result_dic[d]))
print 'over'
res.close()

exit()
print "end"
