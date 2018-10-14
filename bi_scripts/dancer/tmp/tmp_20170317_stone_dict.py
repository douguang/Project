#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : V1-V6，V7-V10， V11-V14，V15   的活跃玩家
            时间3月16日
            从V1-V15  各VIP等级 活跃玩家身上的宝石等级查询 （上阵6位和9位）
'''
import pandas as pd
import numpy as np
from utils import hql_to_df, update_mysql, ds_add, get_config, date_range
import settings_dev
from dancer.cfg import zichong_uids

zichong_uids = str(tuple(zichong_uids))

def tmp_20170215_stone_dict(date):

    stone_sql = '''
        select ds, vip, user_id, stone_dict
        from parse_info
        where ds = '{date}' and vip>0
        and user_id not in {zichong_uids}
    '''.format(**{
        'date': date, 'zichong_uids': zichong_uids
    })

    print stone_sql

    # 获取秘籍配置
    stone_config = get_config('stone_att')
    stone_df = hql_to_df(stone_sql)

    # 把stone_dict 展开，每一个秘籍合并其它几个数据变为一行
    def stone_lines():
        for _, row in stone_df.iterrows():
            for stone_pos, stone_info in eval(row.stone_dict).iteritems():
                if stone_pos == 'stone_pos':
                    for pos, stone in stone_info.iteritems():
                        if int(pos) in range(0, 6, 1):
                            for id in stone:
                                id = str(id)
                                name = stone_config.get(id, {}).get('name')
                                # print name
                                yield [row.ds, row.user_id, row.vip, pos, id, name]

    stone_all_df = pd.DataFrame(stone_lines(), columns=['ds', 'user_id',  'vip', 'pos', 'id', 'name'])
    stone_all_df.to_excel(r'E:\Data\output\dancer\stone_dict_detail.xlsx')
    print stone_all_df.head(30)
    stone_all_df['num'] = 1
    result_df = pd.pivot_table(stone_all_df, values='num', index=['ds', 'vip'], columns='name', aggfunc=np.sum, fill_value=0).reset_index()
    print result_df.head(25)
    return result_df

if __name__ == '__main__':
    for platform in ['dancer_tw']:
        settings_dev.set_env(platform)
        result_list = []
        for date in date_range('20170316', '20170316'):
            result_list.append(tmp_20170215_stone_dict(date))
        result = pd.concat(result_list)
        result.to_excel(r'E:\Data\output\dancer\stone_dict.xlsx')