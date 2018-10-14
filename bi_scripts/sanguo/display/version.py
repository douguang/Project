#!/usr/bin/env python
# -*- coding: UTF-8 -*-
D = {}
txt = '''
220.194.71.250 - - [21/Aug/2016:03:23:46 +0800] "GET /m51/api/?method=mine.index&user_token=m518978376&mk=271&pt=pp&channel_id=&device_mark=3D13FF2F-2466-407C-A37E-87C5106CAE2A&version=1.1.7&__ts=1471721028&platform_channel=pp&device_mem=1048203264&rn=1471721028661& HTTP/1.1" - 200 2716 "-" "-" "-" 10.253.9.154:5400 0.054 0.054

42.73.202.23 - - [21/Aug/2016:03:23:46 +0800] "GET /gloryroad_master/login/?method=point_log&&logName=getPlatForm&recordType=once&channel=kv_game&deviceMac=wifi02%3A00%3A00%3A00%3A00%3A00&deviceName=D6653&deviceVersion=6.0.1&deviceTime=1471720952363&deviceNet=MOBILE& HTTP/1.1" - 200 0 "-" "Dalvik/2.1.0 (Linux; U; Android 6.0.1; D6653 Build/23.5.A.0.575)" "-" - - 0.000

117.136.84.172 - - [21/Aug/2016:03:23:46 +0800] "GET /m83/api/?method=mine.auto_find&user_token=m831029726&mk=14&pt=msdk&channel_id=&device_mark=wifi24:1f:a0:7d:a2:9b&version=1.1.7&__ts=1471721025&platform_channel=msdk&device_mem=2927947776&sort=1&rn=1471721024790&mine_grade=4& HTTP/1.1" - 200 903 "-" "-" "-" 10.254.164.171:5400 0.029 0.029
'''
user_id = txt.split('user_token=')[1].split('&mk')[0]
version = txt.split('version=')[1].split('&rn')[0]
key = '%s\t%s' % (user_id,version)
if key not in D:
    D[key] = ''
else:
    pass

