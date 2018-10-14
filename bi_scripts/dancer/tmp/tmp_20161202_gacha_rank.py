#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 武娘国服 10月30号 参加限时神将活动的玩家的消耗(该脚本中只取出了抽奖记录)
'''

import settings_dev
from utils import hql_to_df
from pandas import DataFrame

def tmp_20161202_gacha_rank(date):

    info_sql = '''
      select
        user_id, reg_time, free_coin, charge_coin , vip, ds
      from
        parse_info
      where
        ds in {date}
    '''.format(date=date)

    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head(10)
    # 限时神将
    # spend_sql = '''
    #   select
    #     user_id, sum(coin_num) as money
    #   from
    #     raw_spendlog
    #   where
    #     ds = '{date}' and goods_type like '%gacha%'
    #   group by
    #     user_id
    # '''.format(date=date)
    # 无量宝藏
    # spend_sql = '''
    #       select
    #         user_id, sum(coin_num) as money
    #       from
    #         raw_spendlog
    #       where
    #         ds = '{date}' and goods_type like '%oracle%'
    #       group by
    #         user_id
    #     '''.format(date=date)
    # 北冥之灵
    # spend_sql = '''
    #       select
    #         user_id, sum(coin_num) as money, ds
    #       from
    #         raw_spendlog
    #       where
    #         ds in {date} and goods_type like '%magic_school%'
    #       group by
    #         user_id, ds
    #     '''.format(date=date)
    # 幸运轮盘
    spend_sql = '''
          select
            user_id, sum(coin_num) as money, ds
          from
            raw_spendlog
          where
            ds in {date} and goods_type like '%roulette%'
          group by
            user_id, ds
        '''.format(date=date)
    # 武道会
    # spend_sql = '''
    #       select
    #         user_id, sum(coin_num) as money, ds
    #       from
    #         raw_spendlog
    #       where
    #         ds = '{date}' and goods_type like '%super_active%'
    #       group by
    #         user_id, ds
    #     '''.format(date=date)
    #
    #藏宝阁
    # spend_sql = '''
    #       select
    #         user_id, sum(coin_num) as money, ds
    #       from
    #         raw_spendlog
    #       where
    #         ds = '{date}' and goods_type like '%black_shop%'
    #       group by
    #         user_id, ds
    #     '''.format(date=date)


    print spend_sql
    spend_df = hql_to_df(spend_sql)
    print spend_df.head(10)

    result = spend_df.merge(info_df, on=(['user_id', 'ds']), how='left')
    print result.head(10)

    return result


if __name__ == '__main__':
    settings_dev.set_env('dancer_pub')
    date = ('20161204', '20161205')
    result = tmp_20161202_gacha_rank(date)
    print result.head(5)
    result.to_excel('/home/kaiqigu/Documents/lunpan.xlsx')