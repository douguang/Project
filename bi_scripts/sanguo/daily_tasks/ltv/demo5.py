#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: demo5.py 
@time: 17/12/25 下午4:57 
"""

import requests

# POST https://www.ipip.net/ip/ajax/ HTTP/1.1
# Host: www.ipip.net
# Connection: keep-alive
# Content-Length: 29
# Accept: */*
# Origin: https://www.ipip.net
# X-Requested-With: XMLHttpRequest
# User-Agent: Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36
# Content-Type: application/x-www-form-urlencoded; charset=UTF-8
# Referer: https://www.ipip.net/ip.html
# Accept-Encoding: gzip, deflate, br
# Accept-Language: zh-CN,zh;q=0.9
# Cookie: acw_tc=AQAAAMK2unL5qwYAIh3PfDVfnXtTPMQy; lang=CN; LOVEAPP_SESSID=7fbadd130d324338924aef256495114cd7b4d5c6; _ga=GA1.2.2147225400.1514191002; _gid=GA1.2.1715884698.1514191002; _gat=1
#
# type=baidu&ip=171.253.135.224
#
# Date: Mon, 25 Dec 2017 08:32:05 GMT
# Content-Type: text/html; charset=utf-8
# Connection: keep-alive
# Vary: Accept-Encoding
# Cache-Control: no-cache
# Set-Cookie: lang=CN; expires=Wed, 24-Jan-2018 08:32:05 GMT; Max-Age=2592000; path=/; domain=.ipip.net; secure
# Set-Cookie: lang=CN; expires=Wed, 24-Jan-2018 08:32:05 GMT; Max-Age=2592000; path=/; domain=.ipip.net; secure
# Content-Length: 0

print(requests.get('https://www.ipip.net/ip.html',headers={
'Accept-Encoding':'gzip, deflate, br',
'Accept-Language':'zh-CN,zh;q=0.9',
'Cache-Control':'no-cache',
'Connection':'keep-alive',
'Host':'www.ipip.net',
'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
}).text)




#
# GET https://trace.rtbasia.com/tkj?rta_k=8QUnu0qsgQ&rta_sr=1366x768&rta_page=https%3A%2F%2Fip.rtbasia.com%2Fwebservice%2Fipip%3Fipstr%3D171.253.135.224&rta_rf=https%3A%2F%2Fwww.ipip.net%2Fip.html&rta_brlied=0&rta_crc=vd%3DGoogle%2520Inc.%7Cd_md%3D%7Ca_nm%3DNetscape%7Cp_sub%3D20030107%7Clg%3Dzh-CN%7Cc_depth%3D24%7Cp_ratio%3D1%7Creso%3D1366x768%7Ca_reso%3D1366x738%7Ct_os%3D-480%7Cs_sta%3D1%7Cl_sta%3D1%7Ci_db%3D1&callback=RTB_861514194181840 HTTP/1.1
# Host: trace.rtbasia.com
# Connection: keep-alive
# User-Agent: Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36
# Accept: */*
# Referer: https://ip.rtbasia.com/webservice/ipip?ipstr=171.253.135.224
# Accept-Encoding: gzip, deflate, br
# Accept-Language: zh-CN,zh;q=0.9


print '**************'
print(requests.get('https://trace.rtbasia.com/tkj?rta_k=8QUnu0qsgQ&rta_sr=1366x768&rta_page=https%3A%2F%2Fip.rtbasia.com%2Fwebservice%2Fipip%3Fipstr%3D171.253.135.224&rta_rf=https%3A%2F%2Fwww.ipip.net%2Fip.html&rta_brlied=0&rta_crc=vd%3DGoogle%2520Inc.%7Cd_md%3D%7Ca_nm%3DNetscape%7Cp_sub%3D20030107%7Clg%3Dzh-CN%7Cc_depth%3D24%7Cp_ratio%3D1%7Creso%3D1366x768%7Ca_reso%3D1366x738%7Ct_os%3D-480%7Cs_sta%3D1%7Cl_sta%3D1%7Ci_db%3D1&callback=RTB_861514194181840 HTTP/1.1',headers={
'Accept-Encoding':'gzip, deflate, br',
'Accept-Language':'zh-CN,zh;q=0.9',
'Cache-Control':'no-cache',
'Connection':'keep-alive',
'Host':'trace.rtbasia.com',
'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
}).text)

print '-------------'
print(requests.get('https://www.ipip.net/ip/ajax/',headers={
'Accept-Encoding':'gzip, deflate, br',
'Accept-Language':'zh-CN,zh;q=0.9',
'Cache-Control':'no-cache',
'Connection':'keep-alive',
'Host':'www.ipip.net',
'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
'Request Method':'POST',
'Remote Address':'101.37.43.249:443',
},params={'type':'taobao','ip':'171.253.135.224',}).text)


# Request URL:https://ip.rtbasia.com/webservice/ipip?ipstr=171.253.135.224
# Request Method:GET
# Status Code:200 OK
# Remote Address:175.6.228.156:443
# Referrer Policy:no-referrer-when-downgrade
# Response Headers
# view source
# Connection:keep-alive
# Content-Language:zh-CN
# Content-Type:text/html;charset=UTF-8
# Date:Mon, 25 Dec 2017 09:46:51 GMT
# Server:nginx/1.12.1
# Transfer-Encoding:chunked
# Request Headers
# view source
# Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
# Accept-Encoding:gzip, deflate, br
# Accept-Language:zh-CN,zh;q=0.9
# Connection:keep-alive
# Host:ip.rtbasia.com
# Referer:https://www.ipip.net/ip.html
# Upgrade-Insecure-Requests:1
# User-Agent:Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36
# Query String Parameters
# view source
# view URL encoded
# ipstr:171.253.135.224

print '-------**------'
print(requests.get('https://ip.rtbasia.com/webservice/ipip?ipstr=171.253.135.224',headers={
'Accept-Encoding':'gzip, deflate, br',
'Accept-Language':'zh-CN,zh;q=0.9',
'Cache-Control':'no-cache',
'Connection':'keep-alive',
'Host':'ip.rtbasia.com',
'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
'Request Method':'GET',
}).text)



print '0000000'
# Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
# Accept-Encoding:gzip, deflate, br
# Accept-Language:zh-CN,zh;q=0.9
# Cache-Control:no-cache
# Connection:keep-alive
# Content-Length:57
# Content-Type:application/x-www-form-urlencoded
# Cookie:lang=CN; acw_tc=AQAAAGGjinUe3gIAVcLbZ0k22UVk0ivV; LOVEAPP_SESSID=9f5b5b70023608078248976438d762d9a220963a; _ga=GA1.2.1040544908.1514193797; _gid=GA1.2.2036578524.1514193797; _gat=1
# Host:www.ipip.net




# Upgrade-Insecure-Requests:1
# User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36
print(requests.get('https://www.ipip.net/ip/ajax/',headers={
'Accept-Encoding':'gzip, deflate, br',
'Accept-Language':'zh-CN,zh;q=0.9',
'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8',
'Cache-Control':'no-cache',
'Connection':'keep-alive',
'Host':'www.ipip.net',
'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
'Referer':'https://www.ipip.net/ip.html',
}, params={'type':'taobao','ip':'171.253.135.224'}).text)

