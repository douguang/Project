#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 辅助函数
Database    : dancer_ks
'''
import os
import time
import urllib
import random
import string
import json
import datetime
import pandas as pd
from hdfs import InsecureClient
from multiprocessing.dummy import Pool
import sqlalchemy
import settings_dev
from settings_dev import mysql_template, hive_template, impala_template

def download_config(config_name):
    '''下载配置到当前文件夹'''
    dir_path = os.path.join(settings_dev.BASE_ROOT, 'config', settings_dev.platform)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    URL = settings_dev.url
    data = {
        'url': URL,
        'config_name': config_name
    }
    config_url = '{url}/config/?method=all_config&config_name={config_name}'.format(**data)
    # print config_url
    text = urllib.urlopen(config_url).read()
    d = json.loads(text)
    config_path = os.path.join(dir_path, config_name)
    with open(config_path, 'w') as f:
        json.dump(d['data'][config_name], f, indent=4)

def get_server_list():
    URL = settings_dev.url
    server_list_url = '{url}/config/?method=server_list'.format(url=URL)
    server_list = json.loads(urllib.urlopen(server_list_url).read())
    return {server_dic['server']: server_dic['open_time'] for server_dic in server_list['data']['server_list']}

def get_config(config_name):
    '''返回obj格式的配置'''
    dir_path = os.path.join(settings_dev.BASE_ROOT, 'config', settings_dev.platform)
    config_path = os.path.join(dir_path, config_name)
    if not os.path.isfile(config_path):
        download_config(config_name)
    with open(config_path) as f:
        return json.load(f)
