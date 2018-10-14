#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  解析nginx
@software: PyCharm 
@file: parse_nginx.py 
@time: 18/1/21 下午4:43 
"""

import json
from utils import timestamp_datetime

def parse_nginx(line):
    '''解析Nginx日志，提取部分字段，并转化为 tab分割 格式
    >>>
    '''
    if 'HTTP/1.1" - 404' not in line:
        ip = line.split(' -')[0].strip()
        time = line.split('[')[1].split('-')[0].strip()
        gmt = line.split('[')[1].split('-')[1].split("] ")[0].strip()
        get_post = line.split('"')[1].split(' /')[0].strip()
        api_type = line.split(' /')[1].split('?')[0].strip()
        if 'initLog=' in line:
            initLog = line.split('initLog=')[1].split('&')[0].strip()
        else:
            initLog = ''
        if 'recordType=' in line:
            recordType = line.split('recordType=')[1].split('&')[0].strip()
        else:
            recordType = ''
        if 'eventId=' in line:
            eventId = line.split('eventId=')[1].split('&')[0].strip()
        else:
            eventId = ''
        if 'channel=' in line:
            channel = line.split('channel=')[1].split('&')[0].strip()
        else:
            channel = ''
        if 'deviceMac=' in line:
            deviceMac = line.split('deviceMac=')[1].split('&')[0].strip()
        else:
            deviceMac = ''
        if 'deviceName=' in line:
            deviceName = line.split('deviceName=')[1].split('&')[0].strip()
        else:
            deviceName = ''
        if 'deviceVersion=' in line:
            deviceVersion = line.split('deviceVersion=')[1].split('&')[0].strip()
        else:
            deviceVersion = ''
        if 'deviceTime=' in line:
            deviceTime = timestamp_datetime(int(line.split('deviceTime=')[1].split('&')[0].strip())/1000)
        else:
            deviceTime = ''
        if 'deviceNet=' in line:
            deviceNet = line.split('deviceNet=')[1].split('&')[0].strip()
        else:
            deviceNet = ''

        # 新添加的
        if 'version=' in line:
            version = line.split('version=')[1].split('&')[0].strip()
        else:
            version = ''
        if 'res_version=' in line:
            res_version = line.split('res_version=')[1].split('&')[0].strip()
        else:
            res_version = ''
        if 'device_mk=' in line:
            device_mk = line.split('device_mk=')[1].split('&')[0].strip()
        else:
            device_mk = ''
        if 'device_mark=' in line:
            device_mark = line.split('device_mark=')[1].split('&')[0].strip()
        else:
            device_mark = ''
        if 'uuid=' in line:
            uuid = line.split('uuid=')[1].split('&')[0].strip()
        else:
            uuid = ''
        if 'appid=' in line:
            appid = line.split('appid=')[1].split('&')[0].strip()
        else:
            appid = ''
        if 'pt=' in line:
            pt = line.split('pt=')[1].split('&')[0].strip()
        else:
            pt = ''
        if '__ts=' in line:
            ts = line.split('__ts=')[1].split('&')[0].strip()
        else:
            ts = ''
        if 'os=' in line:
            os = line.split('os=')[1].split(' HTTP')[0].strip()
        else:
            os = ''
        if 'table_name=' in line:
            table_name = line.split('table_name=')[1].split('&')[0].strip()
        else:
            table_name = ''
        if 'key=' in line:
            key = line.split('key=')[1].split('200 ')[0].strip()
        else:
            key = ''
        if 'account=' in line:
            account = line.split('account=')[1].split('&')[0].strip()
        else:
            account = ''
        print 'key:',key
        parsed_line = '\t'.join(map(str, [ip, time, gmt, get_post, api_type, initLog, eventId, recordType, channel, deviceMac, deviceName, deviceVersion, deviceTime, deviceNet,version,res_version,device_mk,device_mark,uuid,appid,pt,ts,os,table_name,key,account])) + '\n'
        return parsed_line

if __name__ == '__main__':
    log = open('/Users/kaiqigu/Documents/slg/access.log_20180120')
    result_list=[]
    for i in log:
        print '-----------'
        print i
        a = parse_nginx(i)
        print a