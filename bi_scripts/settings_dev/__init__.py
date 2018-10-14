#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description :
'''
import os

# 数据库连接 url 模板
mysql_template = 'mysql+pymysql://root:60aa954499f7ab@192.168.1.27/{db}?charset=utf8'
hive_template = 'hive://192.168.1.8:10000/{db}'
impala_template = 'impala://192.168.1.47:21050/{db}'

CUR_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_ROOT = os.path.dirname(CUR_DIR) + os.path.sep
hdfs_url = 'http://192.168.1.8:50070'
# 导入hive文件的位置
hive_path = '/user/hive/warehouse/{db}.db/{table}/ds={date_str}/{filename}'

def set_env(platform):
    '''设置环境
    '''
    # PLATFORMS = ('sanguo_ks', 'sanguo_tx', 'sanguo', 'superhero_bi', 'superhero_vt', 'tf_jinshan')
    # assert platform in PLATFORMS, 'platform must be one of {!s}!'.format(PLATFORMS)
    # 把配置文件里的赋值
    execfile(os.path.join(CUR_DIR, '%s.py' % platform), globals(), globals())
    globals()['platform'] = platform
