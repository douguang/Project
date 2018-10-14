#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  玩家的基本信息
@software: PyCharm 
@file: user_info_data.py 
@time: 18/1/21 下午7:24 
"""
from utils import date_range, ds_add, hql_to_df
import settings_dev
import pandas as pd
from ipip import *
from utils import hqls_to_dfs, get_rank, hql_to_df, date_range
import pandas as pd
from utils import ds_delta
import settings_dev
from pandas import DataFrame
import time
import datetime

import sys
reload(sys)
sys.setdefaultencoding('utf8')

def data_reduce():
    info_sql = '''
        select uid,account,app_id,level,sub_city_num,sb_chapter_id,cur_main_quest,reg_time,offline_time,guide_nodes,sb_pass_id from  mid_info_all
        where ds='20180120' group by uid,account,app_id,level,sub_city_num,sb_chapter_id,cur_main_quest,reg_time,offline_time,guide_nodes,sb_pass_id
    '''
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head()

    nginx_sql = '''
        select account,ip from parse_nginx where ds> ='20180119' and ds<='20180120' and account != '' group by account,ip
    '''
    print nginx_sql
    nginx_df = hql_to_df(nginx_sql)
    print nginx_df.head()

    result_df = info_df.merge(nginx_df, on=['account',], how='left')

    result_df['ip'] = result_df['ip'].astype(basestring)
    IP.load(os.path.abspath("./tinyipdata_utf8.dat"))

    def card_evo_lines():
        for _, row in result_df.iterrows():
            guide_nodes = eval(row['guide_nodes'])
            guide_id = ''
            if type(guide_nodes) == list and len(guide_nodes) != 0:
                guide_id = max(guide_nodes)
            elif type(guide_nodes) == dict:
                value_list = []
                for i in guide_nodes.keys():
                    value_list.append(guide_nodes[i])
                guide_id = max(value_list)
            # IP
            ip = row.ip
            country = ''
            try:
                country = IP.find(ip).strip().encode("utf8")
                if '中国台湾' in country:
                    country = '台湾'
                elif '中国香港' in country:
                    country = '香港'
                elif '中国澳门' in country:
                    country = '澳门'
                elif '中国' in country:
                    country = '中国'
            except:
                pass
            yield [row.uid,row.account,row.app_id,row.level,row.sub_city_num,row.sb_chapter_id,row.cur_main_quest,row.reg_time,row.offline_time,row.guide_nodes,row.ip,guide_id,row.sb_pass_id,country]

    result_df = pd.DataFrame(card_evo_lines(), columns=['uid','account','app_id','level','sub_city_num','sb_chapter_id','cur_main_quest','reg_time','offline_time','guide_nodes','ip','guide_id','sb_pass_id','country'])
    return result_df



if __name__ == '__main__':
    for platform in ['slg_mul', ]:
        settings_dev.set_env(platform)
        res = data_reduce()
        res.to_excel(r'E:\slg-mul_user_game_info_20180122.xlsx', index=False)
    print "end"