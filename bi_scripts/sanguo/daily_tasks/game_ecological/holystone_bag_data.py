#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-6-27 下午4:19
@Author  : Andy 
@File    : holystone_bag_data.py
@Software: PyCharm
Description :   圣石数据

select user_id,level,vip,holystone_bag  from mid_info_all where ds='20170626' and act_time >= '2017-04-07 00:00:00'  and holystone_bag != 'NULL' and holystone_bag != '{}'

{'2-1485533354-75yzwF': {'name': u'\u653b\u64ca\u795e\u6728', 'lv': 5, 's_id': 2, 'att': 2, 'num': 275, 'equip': 1, 'owner': '429401-1492261017-Gv52NE', 'entry': 1, 'quality': 3}, '2-1479965160-XWssHx': {'name': u'\u653b\u64ca\u795e\u6728', 'lv': 5, 's_id': 2, 'att': 2, 'num': 275, 'entry': 1, 'owner': u'444124-1496724314-GntGTQ', 'equip': 1, 'quality': 3}, '2-1490409066-a7en5b': {'name': u'\u653b\u64ca\u795e\u6728', 'lv': 7, 's_id': 2, 'att': 2, 'num': 475, 'entry': 1, 'owner': '517102-1495723620-HuCiKs', 'equip': 1, 'quality': 3}, '2-1485589021-nuTTIZ': {'name': u'\u653b\u64ca\u795e\u6728', 'lv': 5, 's_id': 2, 'att': 2, 'num': 275, 'equip': 1, 'owner': '507106-1494251073-qdX3Jf', 'entry': 1, 'quality': 3}}


'''

import settings_dev
import pandas as pd
from utils import hql_to_df,ds_add, get_config, date_range
import json

def holystone_bag_data(start_ds,end_ds,start_time):
    print start_ds,end_ds,start_time
    # 近两个月活跃的玩家的神器数据
    user_info_sql = '''
     select user_id,level,vip,holystone_bag from mid_info_all where ds='20170628' and act_time >= '2017-06-25 00:00:00'  and holystone_bag != 'NULL'
    '''.format(start_ds=start_ds,end_ds=end_ds,start_time=start_time)
    print user_info_sql
    user_info_df = hql_to_df(user_info_sql)
    print user_info_df

    # 排除VIP12以上的
    result_df = user_info_df[user_info_df['vip'] <= 12]
    result_df = result_df[result_df['holystone_bag'] != '{}']
    print result_df

    user_id_list, level_list, vip_list, name_list, lv_list, quality_list, num_list, attr_list,  = [], [], [], [],[], [], [], []
    for i in range(len(user_info_df)):
        # try:
        user_id = user_info_df.iloc[i, 0]
        level = user_info_df.iloc[i, 1]
        vip = user_info_df.iloc[i, 2]
        holystone_bag = user_info_df.iloc[i, 3]
        print '000000000000'
        print holystone_bag
        print type(holystone_bag)
        if holystone_bag != '{}':
            # artifact_lv = json.loads(json.dumps(eval(holystone_bag.decode("unicode-escape"))))
            artifact_lv = json.loads(json.dumps(eval(holystone_bag.decode("unicode-escape"))))
            print dict(artifact_lv).values()
            for value in dict(artifact_lv).values():
                print type(value)
                print value
                name=''
                lv=''
                att = ''
                num = ''
                quality = ''
                if u'name' in value.keys():
                    name = value[u'name']
                if u'lv' in value.values():
                    lv = value[u'lv']
                if u's_id' in value.values():
                    print value[u's_id']
                if u'att' in value.values():
                    att = value[u'att']
                if u'num' in value.values():
                    num = value[u'num']
                if u'equip' in value.values():
                    print value[u'equip']
                if u'owner' in value.values():
                    print value[u'owner']
                if u'entry' in value.values():
                    print value[u'entry']
                if 'quality' in value.values():
                    quality = value[u'quality']
                print '-------------'
                print value[u'name']
                print name
                print '-------------'
                print lv
                print value[u'lv']
                print '-------------'
                print att
                print value[u'att']
                print '-------------'
                print num
                print value[u'num']
                print '-------------'
                print quality
                print value[u'quality']
                user_id_list.append(user_id)
                level_list.append(level)
                vip_list.append(vip)
                name_list.append(value[u'name'])
                lv_list.append(value[u'lv'])
                quality_list.append(value[u'quality'])
                num_list.append(value[u'num'])
                attr_list.append(value[u'att'])

    result_df = pd.DataFrame({'user_id': user_id_list,
                              'level': level_list,
                              'vip': vip_list,
                              'name': name_list,
                              'lv': lv_list,
                              'quality': quality_list,
                              'num': num_list,
                              'attr': attr_list,})
    #
    # print result_df
    # 分组统计

    return result_df

if __name__ == '__main__':
    settings_dev.set_env('sanguo_ks')
    start_ds = ''
    end_ds = '20170629'
    start_time = '2017-06-25 00:00:00'
    user_info_df = holystone_bag_data(start_ds,end_ds,start_time)
    user_info_df.to_excel('/home/kaiqigu/桌面/机甲无双-金山版-圣石数据_20170630.xlsx', index=False)
    print "end"



