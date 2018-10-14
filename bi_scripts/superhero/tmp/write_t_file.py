#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 将空格分割的文件 转换成 /t 文件
注：20160824日mid_ser_map字段顺序：主服-从服
'''

path = r'/Users/kaiqigu/Downloads/'

file_in = path +  'vn_pub.txt'
file_out = path + 'mid_ser_map_vn_20161102'

f_in = open(file_in,'r')
f_out = open(file_out,'w')
print f_in
print f_out

for l_raw in f_in:
    try:
        line = l_raw.strip().split(' ')
        son_ser = line[0]
        father_ser = line[len(line)-1]
        print son_ser,father_ser
        f_out.write(str(son_ser) + '\t' + str(father_ser) + '\n')
    except:
        print 'error !!!'

f_in.close()
f_out.close()
