#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Desctiption : 游戏上线次日数据需求
Time        : 2017-03-17
'''
import pandas as pd
from pandas import DataFrame

import settings_dev
from utils import hql_to_df, date_range


def get_data(date):
    settings_dev.set_env('jianniang_test')

    # 卡牌拥有量
    # card_sql = '''
    # SELECT {0} as ds,
    #         card_id,
    #         star,
    #         count(user_id) AS get_num,
    #         count(distinct user_id) AS user_num
    # FROM raw_card
    # WHERE ds = '{0}'
    # GROUP BY  card_id, star
    # '''.format(date)
    # try:
    #     card_df = hql_to_df(card_sql)
    #     print card_df.head()
    #     card_df.to_excel(
    #         r'C:\workflow\bi_scripts\jianniang\tmp\%s_get_cards.xlsx' % date, index=False)
    # except Exception, e:
    #     print 'card error', e
    #     pass

    # 战力，根据玩家身上卡牌计算
    # sword_sql = '''
    # SELECT {0} as ds,
    #         user_id,
    #         count(card_id) AS card_num,
    #         count(distinct card_id) AS card_dup_num,
    #         sum(sword) AS sword
    # FROM raw_card
    # WHERE ds = '{0}'
    # GROUP BY  user_id
    # ORDER BY  sword desc
    # '''.format(date)
    # try:
    #     sword_df = hql_to_df(sword_sql)
    #     print sword_df.head()
    #     sword_df.to_excel(
    #         r'C:\workflow\bi_scripts\jianniang\tmp\%s_user_sword.xlsx' % date, index=False)
    # except:
    #     print 'sword error'
    #     pass

    # 推图停留
    raid_sql = '''
    SELECT {0} as ds,
            user_id,
            day_win_raids,
            raids
    FROM raw_raid
    WHERE ds = '{0}'
    '''.format(date)
    try:
        raid_df = hql_to_df(raid_sql)

        def raid_linse():
            for _, row in raid_df.iterrows():
                for ids in eval(row.raids):
                    try:
                        for idss in ids:
                            yield [row.ds, row.user_id, row.day_win_raids, idss]
                    except Exception, e:
                        print row.user_id, row.raids
                        print e
        n_raid_df = pd.DataFrame(
            raid_linse(),
            columns=['ds', 'user_id', 'day_win_raids', 'raids']
        )
        print n_raid_df.head()
        n_raid_df.to_excel(
            r'C:\workflow\bi_scripts\jianniang\tmp\%s_user_raids.xlsx' % date, index=False)
    except Exception, e:
        print 'raids error', e
        pass

if __name__ == '__main__':
    for date in date_range('20170317', '20170319'):
        get_data(date)
