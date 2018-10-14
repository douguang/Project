#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : homeland.run_task,玩家家园任务卡牌分布（卡牌id、跑家园数量，可以出近一个星期的）
Name        : tmp_20170502_homeland_task
Original    : tmp_20170502_homeland_task
'''
import settings_dev
import pandas as pd
from utils import hql_to_df, update_mysql, get_config, date_range, ds_add
from collections import Counter
from dancer.cfg import zichong_uids
import time
import datetime

zichong_list = str(tuple(zichong_uids))

def tmp_20170502_homeland_task(date):

    homeland_task_sql = '''
    SELECT user_id,
           vip,
           a_tar
    FROM parse_actionlog
    WHERE ds <= '{date}' and user_id not in {zichong_list} and ds>='{date_ago}' and a_typ='homeland.run_task' and user_id in
    (SELECT user_id FROM mid_info_all WHERE ds = '{date}' and regexp_replace(to_date(act_time), '-', '') >='{date_ago}' and vip=15 and to_date(reg_time)<'2017-03-01')
    '''.format(date=date, date_ago=ds_add(date, -6), zichong_list=zichong_list)
    print homeland_task_sql

    homeland_task_df = hql_to_df(homeland_task_sql)

    info_config = get_config('character_info')
    detail_config = get_config('character_detail')

    # 解析遍历card_dict日志文件
    def homeland_task_lines():
        for _, row in homeland_task_df.iterrows():
            # for cards_id, card_info in eval(row['a_tar']).iteritems():
            tar = eval(row['a_tar'])
            # print type(tar), tar
            card_id = tar['card_id'].split('-')[0]
            # print card_id
            character_id = str(
                detail_config.get(
                    card_id, {}).get('character_id'))
            c_name = info_config.get(character_id, {}).get('name')
            yield [row.user_id, row.vip, character_id, c_name]

    card_info_df = pd.DataFrame(homeland_task_lines(),
                                columns=['user_id', 'vip_level', 'character_id', 'card_name'])

    # card_info_df.to_excel(r'E:\Data\output\dancer\homeland.xlsx')

    # 统计单卡跑任务数
    use_df = card_info_df.groupby(['character_id', 'card_name']).user_id.count().reset_index().rename(columns={'user_id': 'times'})
    print use_df.head(20)

    use_df.to_excel(r'E:\Data\output\dancer\homeland.xlsx')


if __name__ == '__main__':
    for platform in ('dancer_pub',):
        settings_dev.set_env(platform)
    #     settings_dev.set_env('dancer_tw')
        tmp_20170502_homeland_task('20170501')