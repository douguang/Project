#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: ip_demo2.py 
@time: 17/12/25 上午11:54 
"""
import requests
from lxml import etree
import json
import random
import BeautifulSoup
import sys
reload(sys)
sys.setdefaultencoding("utf-8")


def checkip_a(ip):
    demo = {}
    if ip == '':
        return demo
    demo['ip'] = ip
    demo['country'] = ''
    demo['area'] = ''
    demo['province'] = ''
    demo['city'] = ''
    demo['isp'] = ''

    URL = 'http://ip.taobao.com/service/getIpInfo.php'
    try:
        r = requests.get(URL, params=ip, timeout=3)
        print r
        print r.content
    except requests.RequestException as e:
        print(e)
    else:
        print r.content
        print '--------'

        tree = etree.HTML(r.content)

        nodes = tree.xpath("//div[@id='result']")
        print(nodes[0]).text


        soup = BeautifulSoup(r.content)
        print soup.find('<div id="result">')

        json_data = r.json()
        print r
        if json_data[u'code'] == 0:
            demo['country'] = json_data[u'data'][u'country'].encode('utf-8')
            demo['area'] = json_data[u'data'][u'area'].encode('utf-8')
            demo['province'] = json_data[u'data'][u'region'].encode('utf-8')
            demo['city'] = json_data[u'data'][u'city'].encode('utf-8')
            demo['isp'] = json_data[u'data'][u'isp'].encode('utf-8')

        else:
            print '查询失败,请稍后再试！'



# ip = {'ip': '202.102.193.68'}
ip = '171.253.135.224'
# ip = {'ip': '42.114.16.141'}
checkip_a(ip)