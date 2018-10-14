#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: demo7.py 
@time: 17/12/25 下午6:10 
"""
import requests
from lxml import etree
import json
import random
import BeautifulSoup
import sys
reload(sys)
sys.setdefaultencoding("utf-8")


def checkip_a(ip,type):
    demo = {}
    if ip == '':
        return demo
    demo['ip'] = ip
    demo['country'] = ''
    demo['area'] = ''
    demo['province'] = ''
    demo['city'] = ''
    demo['isp'] = ''

    URL = 'https://www.ipip.net/ip.html'
    try:
        r = requests.get(URL, params={'type':type,'ip':ip}, timeout=3)
        print r
        print r.url
        print r.content
    except requests.RequestException as e:
        print(e)
    else:
        print r.content
        print '--------'

ip = '171.253.135.224'
type = 'taobao'
checkip_a(ip,type)

