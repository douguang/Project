#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description :
'''
from urlparse import urlparse, parse_qs
result = {}
columns = ['get_user_server_list', 'loading', 'all_config', 'new_account',
           'new_user', 'mark_user_login', 'cards.open', 'user.get_camp_num',
           'user.set_camp', 'user.main_page']
with open('/data/nginx/logs/huawei_nginx_18_21.log', 'r') as f_in, open(
        '/home/admin/huwenchao/python_extract_err.txt', 'w') as f_err, open(
            '/home/admin/huwenchao/mac_method_result_huawei.txt',
            'w') as f_out:
    for line in f_in:
        try:
            l = line.split()
            # log_t = datetime.datetime.strptime(l[3][1:], '%d/%b/%Y:%H:%M:%S').timetuple()
            # if not datetime.datetime(2016, 4, 21, 15, 30) < log_t < datetime.datetime(2016, 4, 21, 21, 30):
            log_t = l[3][1:7]
            # if not '22/Apr/2016:15:30:00' <= log_t <= '22/Apr/2016:21:30:00':
            #     continue
            # log_t = time.mktime(datetime.datetime.strptime(l[3][1:], '%d/%b/%Y:%H:%M:%S').timetuple())
            params = parse_qs(urlparse(l[6]).query)
            method = params['method'][0]
            mac = params['device_mark'][0]
            if method in columns:
                f_out.write('\t'.join(map(str, [log_t, mac, method])) + '\n')
            method_mac = result.setdefault(log_t, {}).setdefault(method, {})
            method_mac[mac] = method_mac.get(mac, 0) + 1
        except:
            f_err.write(line)
            # raise

for log_t, result_d in result.iteritems():
    for c in columns:
        tmp = result_d.get(c, {})
        print '%s\t%s\t%s\t%s' % (log_t, c, len(tmp), sum(tmp.itervalues()))
