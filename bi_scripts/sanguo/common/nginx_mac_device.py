#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 从nginx日志中得到mac地址和device的匹配表
'''
from urlparse import urlparse, parse_qs
import time
import datetime
import diskcache as dc
import glob
import gzip

cache = dc.Cache('cache')
done_logs = cache.get('done_logs', set())
mac_device = cache.get('mac_device', {})
log_gzs = sorted([i for i in glob.glob('/data/nginx/logs/access.log*.gz') if i >= '/data/nginx/logs/access.log-20160419.gz'])
# log_gzs = ['/data/nginx/logs/nginx_log.sample']

for log in log_gzs:
    if log in done_logs:
        continue
    with gzip.open('/data/nginx/logs/access.log-20160504.gz', 'rb') as f_in:
    # with open(log) as f_in, open('mac_device.txt', 'w') as f_out:
        for line in f_in:
            try:
                l = line.split()
                # log_t = datetime.datetime.strptime(l[3][1:], '%d/%b/%Y:%H:%M:%S').timetuple()
                # if not datetime.datetime(2016, 4, 21, 15, 30) < log_t < datetime.datetime(2016, 4, 21, 21, 30):
                # log_t = l[3][1:]
                # if not '22/Apr/2016:15:30:00' <= log_t <= '22/Apr/2016:21:30:00':
                #     continue
                # log_t = time.mktime(datetime.datetime.strptime(l[3][1:], '%d/%b/%Y:%H:%M:%S').timetuple())
                params = parse_qs(urlparse(l[6]).query)
                method = params['method'][0]
                if method != 'point_log':
                    continue
                mac = params['deviceMac'][0]
                device = params['deviceName'][0]
                mac_device[mac] = device
                # f_out.write('\t'.join(map(str, [log_t, mac, method]))+'\n')
            except:
                pass
    done_logs.add(log)
    cache['mac_device'] = mac_device
    cache['done_logs'] = done_logs
    with open('mac_device.txt', 'w') as f_out:
        f_out.write(str(done_logs) + '\n\n')
        f_out.write('\n'.join(['%s\t%s' % (k, v) for k, v in mac_device.iteritems()]))
        f_out.write('\n')


