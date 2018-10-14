#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 副本推图，困难难度，每章节的3、6、9关卡
Database    : dancer_pub
'''
import settings_dev
import pandas as pd
from utils import hql_to_df, date_range, ds_add
def tmp_20170215_forward_difficult(date):

    forward_sql = '''
        select
            ds,
            server,
            vip,
            user_id,
            a_tar
        from
            parse_actionlog
        where
            ds='{date}' and
            a_typ='forward.forward'
    '''.format(date=date)
    print forward_sql
    forward_df = hql_to_df(forward_sql)

    user_id_list, server_list, vip_list, chapter_list, stageid_list, win_list, diffculty_list, ds_list = [], [], [], [], [], [], [], []
    for i in range(len(forward_df)):
        user_id_list.append(forward_df.iloc[i, 3])
        ds_list.append(forward_df.iloc[i, 0])
        server_list.append(forward_df.iloc[i, 1])
        vip_list.append(forward_df.iloc[i, 2])
        tar = forward_df.iloc[i, 4]
        tar = eval(tar)
        chapter_list.append(tar['chapter'])
        stageid_list.append(tar['stageid'])
        win_list.append(tar['is_win'])
        diffculty_list.append(tar['diffculty_step'])

    data = pd.DataFrame({'user_id': user_id_list, 'server': server_list, 'vip': vip_list, 'chapter': chapter_list,
                         'stageid': stageid_list, 'is_win': win_list, 'diffculty_step': diffculty_list, 'ds': ds_list})
    print data.head(25)
    # data['num'] = 1
    # result = data.groupby(['user_id', 'server', 'vip', 'ds', 'id']).agg({
    #     'num':lambda g:g.count()
    # }).reset_index()

    return data


if __name__ == '__main__':
    settings_dev.set_env('dancer_pub')
    # result_list = []
    for date in date_range('20170207', '20170213'):
        data = tmp_20170215_forward_difficult(date)
        data.to_csv(r'E:\Data\output\dancer\forward_difficult_%s.csv'%date)
        # result_list.append(date)
    # result = pd.concat(result_list)
    # result.to_excel(r'E:\Data\output\dancer\forward_difficult.xlsx')