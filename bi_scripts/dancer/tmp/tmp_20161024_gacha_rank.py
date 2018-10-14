#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 武娘台湾 10月22号 参加限时神将活动的玩家的积分以及排名(该脚本中只取出了抽奖记录)
'''

import settings_dev
from utils import hql_to_df
from pandas import DataFrame

def tmp_20161022_gacha_rank(date,server):

    gacha_sql = '''
      select
        user_id,server, a_typ, a_tar
      from
        parse_actionlog
      where
        ds = '{date}' and a_typ ='gacha.get_gacha' and server = '{server}'
    '''.format(date=date,server=server)

    print gacha_sql

    gacha_df = hql_to_df(gacha_sql)

    print 'zhuang'

    #限时神将接口为gacha.get_gacha接口，其中a_tar中gacha_sort字段用于记录抽奖类型（普通抽奖或者十连抽等）
    #取出全服所有玩家抽奖记录中的抽奖类型，即可根据类型赋值求积分，进而排名。
    user_id_list, server_list, type_list, gacha_sort_list = [], [], [], []
    for i in range(len(gacha_df)):
        user_id = gacha_df.iloc[i, 0]
        server = gacha_df.iloc[i, 1]
        type = gacha_df.iloc[i, 2]
        tar = gacha_df.iloc[i, 3]
        tar = eval(tar)
        gacha_sort = tar['gacha_sort']
        user_id_list.append(user_id)
        server_list.append(server)
        type_list.append(type)
        gacha_sort_list.append(gacha_sort)
    data = DataFrame({'user_id': user_id_list, 'server': server_list, 'type': type_list, 'gacha_sort': gacha_sort_list})
    columns = ['user_id','server','type','gacha_sort',]
    data = data[columns]

    return data


if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    # server = ('tw23', 'tw26', 'tw30', 'tw31', 'tw32')
    server = 'tw55'
    result = tmp_20161022_gacha_rank('20161226', server)
    print result.head(5)
    result.to_excel('/home/kaiqigu/Documents/rank.xlsx')