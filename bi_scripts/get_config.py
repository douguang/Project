#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 获取配置
'''
import urllib
import json
import os
import settings_dev


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

def get_config(config_name, platform=None):
    '''返回obj格式的配置'''
    dir_path = os.path.join(settings_dev.BASE_ROOT, 'config', settings_dev.platform)
    config_path = os.path.join(dir_path, config_name)
    if not os.path.isfile(config_path):
        download_config(config_name, platform)
    with open(config_path) as f:
        return json.load(f)

if __name__ == '__main__':
    settings_dev.set_env('dancer_bt')
    result = download_config('chain')
    # settings.set_env('dancer_pub')
    # result = download_config('chain')
    # result = get_config('character_detail')
    # result = get_config('equip')
    # result = get_config('spirit_skill_detail')
    # print result
