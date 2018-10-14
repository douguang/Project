#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 新服数据(分服)
Time        : 2017.06.05
illustration: forward.special_forward：精英关卡挑战
forward.special_sweep：精英关卡扫荡
'''
import settings_dev
import pandas as pd
# from utils import ds_add
from utils import hql_to_df
# from utils import date_range

chap_list = range(26, 34)

if __name__ == '__main__':
    settings_dev.set_env('dancer_pub')
    action_sql = '''
    SELECT user_id,
           a_typ,
           a_tar
    FROM parse_actionlog
    WHERE ds='20170603'
      AND a_typ IN ('forward.special_forward',
                    'forward.special_sweep')
    '''
    action_df = hql_to_df(action_sql)

    def get_chapter():
        for _, row in action_df.iterrows():
            chapter = eval(row['a_tar'])['chapter']
            yield [row.user_id, row.a_typ, chapter]
    # 生成DataFrame
    chapter_df = pd.DataFrame(get_chapter(),
                              columns=['user_id', 'a_typ', 'chapter'])
    chapter_df['chapter'] = chapter_df['chapter'].astype('int')
    chapter_result = chapter_df[chapter_df.chapter.isin(chap_list)]
    result_df = chapter_result.groupby(
        ['user_id', 'a_typ']).count().reset_index()

    result_df.to_excel('/Users/kaiqigu/Downloads/Excel/chapter_pub.xlsx')
