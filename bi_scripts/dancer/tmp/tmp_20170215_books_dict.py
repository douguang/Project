#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 秘籍概况
'''
import pandas as pd
from utils import hql_to_df, update_mysql, ds_add, get_config, date_range
from collections import Counter
import settings_dev
from dancer.cfg import zichong_uids

zichong_uids = str(tuple(zichong_uids))

def tmp_20170215_books_dict(date):

    books_sql = '''
        select vip, user_id, books_dict
        from mid_info_all
        where ds = '{date}' and to_date(act_time)>='2017-03-27' and user_id not in {zichong_uids}
    '''.format(**{
        'date': date, 'zichong_uids': zichong_uids
    })

    print books_sql

    # 获取秘籍配置
    detail_config = get_config('books')

    books_df = hql_to_df(books_sql)
    books_df.to_excel(r'E:\Data\output\dancer\books_df.xlsx')
    books_df['server'] = books_df['user_id'].map(lambda s: s[:-7])

    # 上阵秘籍位置
    books_shangzheng_pos = set(range(9))

    # 把books_dict 展开，每一个秘籍合并其它几个数据变为一行
    def books_lines():
        for _, row in books_df.iterrows():
            for books_id, books_info in eval(row.books_dict).iteritems():
                c_id = str(books_info['c_id'])
                is_shangzhen = books_info['pos'] in books_shangzheng_pos
                evo_num = books_info['evo']
                yield [row.vip, row.server, row.user_id, c_id, is_shangzhen, evo_num]

    books_all_df = pd.DataFrame(books_lines(), columns=['vip', 'server', 'user_id', 'c_id', 'is_shangzhen', 'evo_num'])
    # books_all_df.to_excel(r'E:\Data\output\dancer\books_evo_detail.xlsx')
    print books_all_df.head(25)

    # 最后索引名字加进去
    # grouped_result_df['card_id'] = grouped_result_df['c_id']
    books_all_df['name'] = books_all_df['c_id'].map(
        lambda x: detail_config[str(x)]['name'])
    print books_all_df.head(25)
    return books_all_df

if __name__ == '__main__':
    for platform in ['dancer_tw']:
        settings_dev.set_env(platform)
        result = tmp_20170215_books_dict('20170328')
        result.to_excel(r'E:\Data\output\dancer\books_evo.xlsx')