#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
import settings
from utils import hql_to_df, update_mysql, ds_add
from pandas import Series,DataFrame
from utils import get_config
import pandas as pd

r1 =  pd.read_table('/Users/kaiqigu/Downloads/vt_action_log_20160427',names=[u'timestamp', u'url_partition', u'server', u'uid', u'pre_vip',
       u'pre_level', u'pre_exp1', u'pre_silver', u'pre_coin',
       u'pre_association_dedication', u'pre_exp2', u'pre_crystal', u'pre_food',
       u'pre_energy', u'pre_metal', u'pre_metalcore', u'pre_action_point',
       u'pre_dirt_silver', u'pre_dirt_gold', u'pre_vip_exp', u'pre_star',
       u'pre_cmdr_energy', u'post_vip', u'post_level', u'post_exp1',
       u'post_silver', u'post_coin', u'post_association_dedication',
       u'post_exp2', u'post_crystal', u'post_food', u'post_energy',
       u'post_metal', u'post_metalcore', u'post_action_point',
       u'post_dirt_silver', u'post_dirt_gold', u'post_vip_exp', u'post_star',
       u'post_cmdr_energy', u'action', u'rc', u'args', u'act_time',
       u'zhongshendian', u'ds'])
print r1

# settings.set_env('superhero_vt')
# print 'please wait a minuate'
# sql = "select * from raw_action_log where ds = '20160427'"
#           tt = hql_to_df(sql)
