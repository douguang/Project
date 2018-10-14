#!/usr/bin/env python
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: demo4.py 
@time: 17/12/25 下午4:25 
"""

import requests
from lxml import etree
import json
import random
import BeautifulSoup
import sys

def checkip_b(ip):
    URL = 'http://www.youdao.com/smartresult-xml/search.s?type=ip&q=%s' % ip
    URL = 'http://fw.qq.com/ipaddress/?ip=%s' % ip
    URL = 'https://www.ipip.net/ip.html/?ip=%s' % ip
    URL = 'https://www.ipip.net/ip/ajax/?type=taobao&ip=%s' % ip
    # URL = 'https://www.ipip.net/ip.html/?type=sina&ip=%s' % ip
    # URL = 'https://ip.rtbasia.com/webservice/ipip?ipstr=%s' % ip
    try:
        r = requests.get(URL, params=ip, timeout=3)
        print r
        print r.url
        print r.content
    except requests.RequestException as e:
        print(e)
    else:
        print r
        print r.request
        print r.url
        # print r.content
        print r.content.decode(encoding='GBK').encode('utf-8')
        # print type(r.content.decode(encoding='GBK').encode('utf-8'))
        # str_data = r.content.decode(encoding='utf-8').encode('utf-8')
        # print str_data
        # str_data = str_data.split('localAddress=')[0]
        # print str_data
        # print json.dumps(str_data)
        # json_data = json.dumps(str_data)
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
