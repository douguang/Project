#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 从Excel中读取卡牌数据并生成/t分割的文件
'''
import pandas as pd

df = pd.read_excel("/home/data/bi_scripts/superhero/cfg_card/cfg_card.xlsx")

df.to_csv('/home/data/bi_scripts/superhero/cfg_card/cfg_card', sep = '\t', index = False, header = False)

# path = r'/Users/kaiqigu/Downloads/'
# file_out = path + 'cfg_card'
# f_out = open(file_out,'w')

# for _, row in df.iterrows():
#     f_out.write(str(row[0]) + '\t' + str(row[1]).encode('utf-8') + '\n')

# f_out.close()
