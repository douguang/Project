#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 比武招亲活动捐献材料统计
'''

from utils import hql_to_df, ds_add, date_range
import settings_dev
import pandas as pd

def tmp_20170504_zhaoqin(date):

    sql = '''
        select user_id, a_rst from parse_actionlog where ds='{date}' and a_typ='contest_zhaoqin.give_item'
    '''.format(date=date)
    print sql
    df = hql_to_df(sql)

    user_id_list, item_list, num_list = [], [], []
    for i in range(len(df)):
        rst = eval(df.iloc[i, 1])
        for j in range(len(rst)):
            item_rst = rst[j]
            user_id_list.append(df.iloc[i, 0])
            k = item_rst.keys()[2]
            item_list.append(item_rst[k])
            num_list.append(item_rst['diff'])
    result_df = pd.DataFrame({'user_id': user_id_list, 'item': item_list, 'num': num_list})
    columns = ['user_id', 'item', 'num']
    result_df = result_df[columns]
    print result_df.head(10)
    return result_df


if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    tmp_20170504_zhaoqin('20170501').to_excel(r'E:\Data\output\dancer\zhaoqin_item.xlsx')