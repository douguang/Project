#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: demo_a.py 
@time: 17/12/26 上午9:55 
"""

import requests
from lxml import etree
import json
import random
import BeautifulSoup
import sys
reload(sys)
sys.setdefaultencoding("utf-8")


a = requests.post('https://www.ipip.net/ip.html',headers={
'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
'Accept-Encoding':'gzip, deflate, br',
'Accept-Language':'zh-CN,zh;q=0.9,en;q=0.8',
'Cache-Control':'max-age=0',
'Connection':'keep-alive',
'Content-Type':'application/x-www-form-urlencoded',
'Cookie':'lang=CN',
'Host':'www.ipip.net',
'Origin':'https://www.ipip.net',
'Pragma':'no-cache',
'Referer':'https://www.ipip.net/ip.html',
'Upgrade-Insecure-Requests':'1',
'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
},data={'_verify':'a42245fccad5f01509d9aba14698eb8f','ip':'115.239.212.137',}).text
# },data={'_verify':'d96d931d966095e6647e64938e17e22f','ip':'171.239.148.103',}).text)
print a
demo ={}
tree = etree.HTML(a)
nodes = tree.xpath("//div[@style='margin: 0 auto;width: 100%;']")
print '--'
print etree.tostring(nodes[0])
print '+++'
print etree.tostring(tree.xpath("//table[@class='table table-striped table-bordered']")[0])
print '000000'
print etree.tostring(tree.xpath("//table[@class='table table-striped table-bordered']")[0].xpath("//div[@style='margin: 0 auto;width: 100%;']")[0])
print '11111111'
print tree.xpath("//table[@class='table table-striped table-bordered']")[0].xpath("//div/span[@id='myself']")[0].text.strip()
print '22222222'
print str(a).split('var datx =')[1].split(';')[0].strip()
print
print type(eval(str(a).split('var datx =')[1].split(';')[0].strip()))
demo['country'] = eval(str(a).split('var datx =')[1].split(';')[0].strip())[0]
demo['area'] = ''
demo['province'] = eval(str(a).split('var datx =')[1].split(';')[0].strip())[1]
demo['city'] = eval(str(a).split('var datx =')[1].split(';')[0].strip())[2]
demo['isp'] = eval(str(a).split('var datx =')[1].split(';')[0].strip())[4]

# print etree.tostring(tree.xpath("//table[@class='table table-striped table-bordered']")[0])
# print etree.tostring(tree.xpath("//table[@class='table table-striped table-bordered']")[0])

# print nodes[0].xpath("//p")[1].xpath("//code")[1].text
# print nodes[0].xpath("//p")[2].text.split(',')[2].strip()
# print nodes[0].xpath("//p")[2].text.split(',')[0].split(':')[1].strip()
# print nodes[0].xpath("//p")[2].text.split(',')[1].strip()
# print nodes[0].xpath("//p")[3].text
