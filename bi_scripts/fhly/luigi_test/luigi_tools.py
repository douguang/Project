#!/usr/bin/env python
# encoding: utf-8

"""
@author: Andy
@site:
@software: PyCharm
@file: demo_parse.py
@time: 17/8/23 上午11:42
"""

def parse_account(line):
    demo_list = line.strip('\n').split(',')
    parsed_line = '\t'.join(map(str, demo_list)) + '\n'
    return parsed_line

def parse_login(line):
    demo_list = line.strip('\n').split(',')
    parsed_line = '\t'.join(map(str, demo_list)) + '\n'
    return parsed_line

def parse_role(line):
    demo_list = line.strip('\n').split(',')
    parsed_line = '\t'.join(map(str, demo_list)) + '\n'
    return parsed_line