#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  根据IP查询详细的地址
@software: PyCharm
@file: ip_tools.py 
@time: 17/12/20 上午11:37 
"""

from lxml import etree
import requests
import json
import random
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

def query_ip(ip):
    demo = {}
    if ip == '':
        return demo
    demo['ip'] = ip
    demo['country'] = ''
    demo['area'] = ''
    demo['province'] = ''
    demo['city'] = ''
    demo['isp'] = ''

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

        URL = 'http://www.ip.cn/index.php?ip=%s' % ip
        try:
            r = requests.get(URL, params=ip, timeout=3)
        except requests.RequestException as e:
            print(e)
        else:
            tree = etree.HTML(r.content)
            nodes = tree.xpath("//div[@id='result']")
            demo['country'] = nodes[0].xpath("//p")[1].xpath("//code")[1].text

            demo['area'] = nodes[0].xpath("//p")[2].text.split(',')[2].strip()
            demo['province'] = nodes[0].xpath("//p")[2].text.split(',')[0].split(':')[1].strip()
            demo['city'] = nodes[0].xpath("//p")[2].text.split(',')[1].strip()

            demo['isp'] = nodes[0].xpath("//p")[3].text


    def checkip_b(ip):
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

    def checkip_c(ip):
        ##
        verify_list = ['a42245fccad5f01509d9aba14698eb8f', 'd96d931d966095e6647e64938e17e22f',
                       'a955d91ea275503768b1fab32668bc0b', '86e638f4fbed6d246ea9a91d3fb1abe9',
                       'cf930895b2f9a2d1da587438f5520fb3']
        verify = random.choice(verify_list)

        html_demo = requests.post('https://www.ipip.net/ip.html', headers={
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Cookie': 'lang=CN',
            'Host': 'www.ipip.net',
            'Origin': 'https://www.ipip.net',
            'Pragma': 'no-cache',
            'Referer': 'https://www.ipip.net/ip.html',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
        }, data={'_verify':verify, 'ip':ip,}).text
        res_list = eval(str(html_demo).split('var datx =')[1].split(';')[0].strip())
        demo['country'] = res_list[0]
        demo['area'] = ''
        demo['province'] = res_list[1]
        demo['city'] = res_list[2]
        demo['isp'] = res_list[4]

    cho_search = [checkip_a,checkip_b,checkip_c]
    while 1:
        random.choice(cho_search)(ip)
        break
    return json.dumps(demo)

if __name__ == '__main__':
    ip = '171.253.135.224'
    demow = query_ip(ip)
    print demow

