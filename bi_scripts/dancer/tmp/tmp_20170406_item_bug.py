#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 刷装备BUG 人数和数量查询  item
'''
from utils import hql_to_df, ds_add, update_mysql, format_dates, date_range, ds_delta
import pandas as pd
import settings_dev

item_list = [4269, 4276, 4283, 4290, 4297, 4304, 4311, 4318, 4352, 4359, 4366, 4373, 4380, 4387, 4394, 4401, 4408, 4415, 4422, 4429, 4436, 4443, 4450, 4457, 4464, 4471, 4478, 4485]


# item_list = [4269,4276]

def tmp_20170205_item_bug(item):
    item_sql = '''
        select '{item}' as item, user_id, log_t, a_typ, a_rst, ds from parse_actionlog where ds>='20161110' and a_rst like '%Item@{item}%'
    '''.format(item=item)
    print item_sql
    item_df = hql_to_df(item_sql)
    # print item_df.head(20)

    item_list, user_id_list, log_t_list, a_typ_list, ds_list, before_list, after_list, diff_list = [], [], [], [], [], [], [], []
    for i in range(len(item_df)):
        rst = eval(item_df.iloc[i, 4])
        # print rst
        for j in range(len(rst)):
            item_rst = rst[j]
            # print item_dict
            if 'Item@%s' % item in str(item_rst):
                # print item_rst
                before_list.append(item_rst['before'])
                after_list.append(item_rst['after'])
                diff_list.append(item_rst['diff'])
                item_list.append(item_df.iloc[i, 0])
                user_id_list.append(item_df.iloc[i, 1])
                log_t_list.append(item_df.iloc[i, 2])
                a_typ_list.append(item_df.iloc[i, 3])
                ds_list.append(item_df.iloc[i, 5])

    data = pd.DataFrame({'item': item_list, 'user_id': user_id_list, 'log_t': log_t_list, 'a_typ': a_typ_list, 'before': before_list, 'after': after_list, 'diff': diff_list, 'ds': ds_list})
    columns = ['user_id', 'log_t', 'a_typ', 'item', 'before', 'after', 'diff', 'ds']
    data = data[columns]
    print data.head(15)
    return data


if __name__ == '__main__':
    for platform in ['dancer_pub']:
        settings_dev.set_env(platform)
        result_list = []
        for item in item_list:
            df = tmp_20170205_item_bug(item)
            df.to_csv(r'E:\Data\output\dancer\item\item_bug_%s.csv' % item)
            result_list.append(df)
        result = pd.concat(result_list)
        result.to_csv(r'E:\Data\output\dancer\item\item_bug_1.csv')