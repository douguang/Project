#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: ip_demo.py 
@time: 17/12/25 上午11:48 
"""

from lxml import etree
import requests
import json
import random
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

def checkip_b(ip):
    demo = {}
    if ip == '':
        return demo
    demo['ip'] = ip
    demo['country'] = ''
    demo['area'] = ''
    demo['province'] = ''
    demo['city'] = ''
    demo['isp'] = ''

    URL = 'http://www.ip.cn/index.php?ip=%s' % ip
    try:
        r = requests.get(URL, params=ip, timeout=3)
    except requests.RequestException as e:
        print(e)
    else:
        print r
        print r.content

        tree = etree.HTML(r.content)
        print tree
        nodes = tree.xpath("//div[@id='result']")
        print '--'
        print nodes[0].tag
        print etree.tostring(nodes[0])
        print nodes[0].attrib
        print nodes[0].xpath("//div[@id='well']")
        print etree.tostring(nodes[0].xpath("//p")[0])
        print etree.tostring(nodes[0].xpath("//p")[1])
        print etree.tostring(nodes[0].xpath("//p")[2])
        print etree.tostring(nodes[0].xpath("//p")[3])

        print nodes[0].xpath("//p")[0].text
        print nodes[0].xpath("//p")[0].xpath("//code")[0].text
        print nodes[0].xpath("//p")[1].text
        print nodes[0].xpath("//p")[1].xpath("//code")[1].text
        print nodes[0].xpath("//p")[2].text
        print nodes[0].xpath("//p")[3].text


        print '**'
        print 'country:',nodes[0].xpath("//p")[1].xpath("//code")[1].text
        print 'area:',nodes[0].xpath("//p")[2].text.split(',')[2]
        print 'province:',nodes[0].xpath("//p")[2].text.split(',')[0].split(':')[1]
        print 'city:',nodes[0].xpath("//p")[2].text.split(',')[1]


        demo['country'] = nodes[0].xpath("//p")[1].xpath("//code")[1].text

        demo['area'] = nodes[0].xpath("//p")[2].text.split(',')[2].strip()
        demo['province'] = nodes[0].xpath("//p")[2].text.split(',')[0].split(':')[1].strip()
        demo['city'] = nodes[0].xpath("//p")[2].text.split(',')[1].strip()

        demo['isp'] = nodes[0].xpath("//p")[3].text

    return demo



# ip = {'ip': '202.102.193.68'}
ip = {'ip': '171.253.135.224'}
ip = {'ip': '42.114.16.141'}
ip = '182.232.77.138'
demo = checkip_b(ip)
print demo