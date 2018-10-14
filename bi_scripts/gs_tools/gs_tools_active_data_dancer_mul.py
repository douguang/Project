#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
活动名         字段名                     配置名             备注
团购活动    group_buy.group_active_buy    contract          {u'count': u'1', u'devicename': u'GN9010L', u'item': u'2', u'channel_id': u'', u'identifier': u'868682020515793'}
灵符活动    magic_school.open_contract    group_version     内含contract_id字段,1为龙符，2为天符，3为风雷符,积分兑换物品为magic_school.contract_exchange内含which字段
限时传奇    gacha.get_gacha               hero_version      抽奖为gacha.get_gacha，单抽返回值为1、2,十连抽为3、4、7、8,1、4没分数，2为10分，其余110
神域活动    oracle_reward.predict         oracle_reward
幸运轮盘    roulette.open_roulette{10}    roulette
宇宙最强/武道会    ****                   super_rich        活动期间 1钻 = 1分
无双商城    shop.unique_shop_buy          unique_shop       排名
"""
import datetime
import time

import pandas as pd
from pandas import DataFrame

import settings_dev
from utils import hql_to_df
from utils import ds_short
from utils import ds_add
from utils import date_range
from utils import get_active_conf
from utils import get_server_active_conf
from utils import update_mysql
from utils import get_server_days
from utils import format_date

import sys


def dancer_ks_active_info(date, platform):

    active_dict = {
        # 'contract': '团购活动',
        'group_version': '灵符活动',
        'hero_version': '限时传奇',
        'oracle_reward': '神域活动/无量宝藏',
        'roulette': '幸运轮盘',
        'super_rich': '宇宙最强/武道会',
        'unique_shop': '无双商城',
        'server_hero_version': '新服-限时传奇',
        'server_roulette': '新服-幸运轮盘',
        'server_super_rich': '新服-宇宙最强/武道会',
        'large_roulette': '跨服幸运轮盘',
        'large_super_rich': '跨服宇宙最强/华山之巅',
    }

    active_typ = {
        # 'contract': 'group_buy.group_active_buy',
        'group_version': 'magic_school.open_contract',
        'hero_version': 'gacha.get_gacha',
        'oracle_reward': 'oracle_reward.predict',
        'roulette': 'roulette.open_%',
        'super_rich': '',
        'unique_shop': 'shop.unique_shop_buy',
        'server_hero_version': 'gacha.get_gacha',
        'server_roulette': 'server_roulette.open_%',
        'server_super_rich': '',
        'large_roulette': 'large_roulette.open_%',
        'large_super_rich': '',
    }

    # 获取服务器开服天数
    server_df = get_server_days(date)
    # print server_df

    def explain(active_name, df):  # 用于解析a_tar等内容,兼容新服活动字段
        for _, row in df.iterrows():
            score = 0
            # 限时传奇
            if active_name.endswith('hero_version'):
                sort_value = eval(row['a_tar']).get('gacha_sort')
                if sort_value in ['3', '5', '6', '7', '8']:
                    score = 110
                elif sort_value in ['2']:
                    score = 10
            # 幸运轮盘
            elif active_name.endswith('roulette'):
                if row.a_typ.endswith('open_roulette10'):
                    score = 110
                else:
                    score = 10
            # 其他无积分活动
            else:
                score = row.spend
            yield [row.user_id, row.server, score, row.spend]

    def get_user_info(date):  # 获取user的info信息
        info_sql = '''
        SELECT t1.user_id,
               name,
               today_pay,
               vip,
               yes_vip,
               yes_coin
        FROM
          (SELECT user_id,
                  name,
                  vip,
                  today_pay
           FROM mart_assist
           WHERE ds = '{0}') t1
        LEFT OUTER JOIN
          (SELECT user_id,
                  vip AS yes_vip,
                  (free_coin + charge_coin) AS yes_coin
           FROM mid_info_all
           WHERE ds = '{1}') t2 ON t1.user_id = t2.user_id
        '''.format(date, ds_add(date, -1))
        info_df = hql_to_df(info_sql)
        info_df = info_df.fillna(0)
        return info_df

    # 当有活动开启时，则执行该函数，输出活动的DataFrame
    def active_detail(active_name, start_time, end_time, date, days=None):
        # 选择服务器
        new_list = []
        if days is None:
            new_server_df = server_df[server_df['days'] > 10]
        else:
            new_server_df = server_df[server_df['days'] == days]
        for _, row in new_server_df.iterrows():
            new_list.append(row.server)
        server_list = str(new_list).replace(
            '[', '(').replace(']', ')').replace('u', '')

        normal_sql = '''
        SELECT user_id,
               server,
               (NVL(freemoney_diff,0) + NVL(money_diff,0)) as spend,
               a_typ,
               a_tar
        FROM parse_actionlog
        WHERE ds >= '{start_short}'
          AND ds <= '{end_short}'
          AND log_t >= '{start_time}'
          AND log_t <= '{end_time}'
          AND a_typ like '{active_name}'
          AND server in {server_list}
          AND return_code = ''
        '''.format(**{
            'start_short': ds_short(start_time),
            'end_short': ds_short(end_time),
            'start_time': start_time,
            'end_time': end_time,
            'active_name': active_typ.get(active_name),
            'server_list': server_list,
        })
        super_sql = '''
        SELECT user_id,
                reverse(substr(reverse(user_id),
                8)) AS server,
                sum(coin_num) AS spend
        FROM raw_spendlog
        WHERE ds >= '{start_short}'
                AND ds <= '{end_short}'
                AND subtime >= '{start_time}'
                AND subtime <= '{end_time}'
                AND reverse(substr(reverse(user_id),8)) IN {server_list}
        GROUP BY  user_id, reverse(substr(reverse(user_id),8))
        '''.format(**{
            'start_short': ds_short(start_time),
            'end_short': ds_short(end_time),
            'start_time': start_time,
            'end_time': end_time,
            'server_list': server_list,
        })

        if active_name.endswith('super_rich'):
            df = hql_to_df(super_sql)
        else:
            df = hql_to_df(normal_sql)
        if df.empty:
            print active_name, start_time, end_time, '活动数据获取失败'
        else:
            explain_df = pd.DataFrame(explain(active_name, df), columns=[
                'user_id', 'server', 'score', 'spend'])
            explain_df = explain_df.groupby(
                ['user_id', 'server']).sum().reset_index()
            # 增加排名列
            explain_df['rank'] = explain_df.groupby(
                ['server'])['score'].rank(method='first', ascending=False)
            sort_df = explain_df.sort_values(
                ['server', 'rank'], ascending=True)
            sort_df['active_name'] = active_dict.get(active_name)
            sort_df['active_date'] = pd.to_datetime(date)
            return sort_df[sort_df['rank'] <= 10]

    # 程序主体
    dfs = []
    act_list = []
    for keys in active_dict:
        try:
            active_name = keys.strip()
            if active_name.startswith('server'):
                for days in range(1, 11):
                    version, act_days, start_time, end_time = get_server_active_conf(
                        active_name, date, str(days))
                    # print version, act_days, start_time, end_time
                    if version != '':
                        try:
                            act_list.append(active_name)
                            print date, '新服活动', act_days, active_dict.get(active_name), '正在查询'
                            act_df = active_detail(
                                active_name, start_time, end_time, date, days)
                            dfs.append(act_df)
                        except Exception as e:
                            print e

            else:
                version, start_time, end_time = get_active_conf(
                    active_name, date)
                if version != '':
                    act_list.append(active_name)
                    print date, '老服活动：', active_dict.get(active_name)
                    act_df = active_detail(
                        active_name, start_time, end_time, date)
                    dfs.append(act_df)
        except Exception, e:
            print '遍历活动数据过程出错'
            print e

    if act_list:
        if len(act_list) > 1:
            concat_df = pd.concat(dfs)
        elif len(act_list) == 1:
            concat_df = pd.DataFrame(dfs)
        # 获取用户信息
        print '正在获取用户信息'
        info_df = get_user_info(date)
        print '正在合并信息'
        finally_df = concat_df.merge(info_df, on='user_id', how='left').merge(
            server_df, on='server', how='left')
        columns_name = ['server', 'days', 'user_id', 'name', 'rank', 'score', 'spend',
                        'today_pay', 'vip', 'yes_vip', 'yes_coin', 'active_name', 'active_date']
        finally_df = finally_df[columns_name]
        print finally_df.head()

        if platform == 'dancer_pub':
            platform_2 = 'dancer_ks'
        else:
            platform_2 = platform
        table = '%s_active_info'%platform_2
        print '更新MySQL表 --->', table
        del_sql = 'delete from {0} where active_date = "{1}"'.format(
            table, format_date(date))
        update_mysql(table, finally_df, del_sql, 'godvs')
    else:
        print date, '未检测到新、老服开启活动内容'


if __name__ == '__main__':
    #d = sys.argv[1]
    #if not d:
    #    d = 1

    ds = (datetime.date.today() - datetime.timedelta(days=1)).isoformat().replace('-', '')
    print ds
    for platform in ['dancer_mul']:
        settings_dev.set_env(platform)
        dancer_ks_active_info(ds, platform)
    # for date in date_range('20170807', '20170829'):
    #     settings_dev.set_env('dancer_mul')
    #     dancer_ks_active_info(date, 'dancer_mul')
