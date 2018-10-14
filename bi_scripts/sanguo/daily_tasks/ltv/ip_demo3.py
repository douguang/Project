#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: ip_demo3.py 
@time: 17/12/25 下午3:59 
"""

import requests
from lxml import etree
import json
import random
import BeautifulSoup
import sys

def checkip_b(ip):
    demo = {}
    URL = 'http://int.dpool.sina.com.cn/iplookup/iplookup.php?format=json&ip=%s' % ip
    try:
        r = requests.get(URL, params=ip, timeout=3)
    except requests.RequestException as e:
        print(e)
    else:
        json_data = eval(r.content.decode(encoding='utf-8').encode('utf-8').strip())
        demo['country'] = json_data['country']
        demo['area'] = json_data['district']
        demo['province'] = json_data['province']
        demo['city'] = json_data['city']
        demo['isp'] = json_data['isp']

        # if json_data[u'code'] == 0:
        #     print '所在国家： ' + json_data[u'data'][u'country'].encode('utf-8')
        #     print '所在地区： ' + json_data[u'data'][u'area'].encode('utf-8')
        #     print '所在省份： ' + json_data[u'data'][u'region'].encode('utf-8')
        #     print '所在城市： ' + json_data[u'data'][u'city'].encode('utf-8')
        #     print '所属运营商：' + json_data[u'data'][u'isp'].encode('utf-8')
        # else:
        #     print '查询失败,请稍后再试！'


# ip = {'ip': '202.102.193.68'}
ip = '171.253.135.224'
ip = '202.102.193.68'
ip = '115.42.238.82'
checkip_b(ip)
