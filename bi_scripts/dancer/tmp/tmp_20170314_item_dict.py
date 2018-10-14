#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : V1-V6，V7-V10， V11-V14，V15   的活跃玩家
材料 ：进阶石   千年参   装备精华  宝物精华  无字卷轴  体悟单  任派强化丹   玫瑰花   金汞剂  银两  元气丹
'''
import pandas as pd
import numpy as np
from utils import hql_to_df, update_mysql, ds_add, get_config, date_range
import settings_dev
from dancer.cfg import zichong_uids

zichong_uids = str(tuple(zichong_uids))

def tmp_20170215_item_dict(date):

    item_sql = '''
        select ds, vip, user_id, item_dict
        from parse_info
        where ds = '{date}' and vip>0
        and user_id not in {zichong_uids}
    '''.format(**{
        'date': date, 'zichong_uids': zichong_uids
    })

    print item_sql

    # 获取秘籍配置
    item_config = get_config('item')
    item_df = hql_to_df(item_sql)

    # 把item_dict 展开，每一个秘籍合并其它几个数据变为一行
    def item_lines():
        for _, row in item_df.iterrows():
            item_dict = eval(row.item_dict)
            for key in item_dict:
                if key in ['190', '92', '110', '130', '400', '210', '280', '35', '700', '4', '34']:
                    name = item_config.get(key, {}).get('name')
                    num = item_dict[key]
                    yield [row.ds, row.vip, row.user_id, name, num]

    item_all_df = pd.DataFrame(item_lines(), columns=['ds', 'vip',  'user_id', 'name', 'num'])
    print item_all_df.head(30)
    result_df = pd.pivot_table(item_all_df, values='num', index=['ds', 'user_id', 'vip'], columns='name', aggfunc=np.sum, fill_value=0).reset_index()
    print result_df.head(25)
    return result_df

if __name__ == '__main__':
    for platform in ['dancer_tw']:
        settings_dev.set_env(platform)
        result_list = []
        for date in date_range('20170327', '20170329'):
            result_list.append(tmp_20170215_item_dict(date))
        result = pd.concat(result_list)
        result.to_excel(r'E:\Data\output\dancer\item_dict.xlsx')