#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
>>> 转化率，使用nginx的deviceMac统计
"""
log = open(r'C:\workflow\bi_scripts\jianniang\tmp\bi_point.log')
result = open(r'C:\workflow\bi_scripts\jianniang\tmp\bi_result.txt', 'a+')
for line in log:
    if 'logName' in line:
        logName = line.split('logName=')[1].split('&')[0].strip()
        deviceMac = line.split('deviceMac=')[1].split('&')[0].strip()
        deviceName = line.split('deviceName=')[1].split('&')[0].strip()
        ip = line.split(' -')[0].strip()
        time = line.split('[')[1].split(' ')[0].strip()
        result.write('%s\t%s\t%s\t%s\t%s\n' %
                     (logName, deviceMac, deviceName, ip, time))

log.close()
result.close()
