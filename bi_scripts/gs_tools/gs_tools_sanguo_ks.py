#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-7-4 上午11:12
@Author  : Andy 
@File    : gs_tools_sanguo_ks.py
@Software: PyCharm
Description :   机甲无双-金山 客服GS系统添加活动玩家信息表
'''

import datetime
import time

import pandas as pd
from pandas import DataFrame

import settings_dev
from utils import hql_to_df,date_range
from utils import ds_add
from utils import update_mysql
from utils import format_date


def active_info(date):
    # 获取服务器开服天数
    server_days_sql = '''
        select reverse(substring(reverse(user_id),8)) as server,regexp_replace(to_date(act_time),'-','')  as ds,max(datediff(to_date(act_time),to_date(reg_time))+1) as act_ds from mid_info_all where ds='{date}' group by server,ds
    '''.format(date=date)
    server_df = hql_to_df(server_days_sql)
    # print server_df.head()

    info_sql = '''
            select ds,user_id,reverse(substr(reverse(user_id), 8)) as server,a_typ,a_tar,log_t
            from parse_actionlog
            WHERE ds = '{date}'
            and a_typ like '%do_limit_gacha%'
            and a_tar like '%gacha_id%'
            group by ds,user_id,reverse(substr(reverse(user_id), 8)),a_typ,a_tar,log_t
    '''.format(date=date)
    info_df = hql_to_df(info_sql)
    # print info_df.head()

    vip_sql = '''
            select ds,user_id,vip from mid_info_all where ds='{date}' group by ds,user_id,vip
        '''.format(date=date)
    vip_df = hql_to_df(vip_sql)
    # print vip_df.head()
    info_df = info_df.merge(vip_df, on=['ds', 'user_id'], how='left')
    info_df = info_df[['ds', 'user_id', 'server', 'vip', 'a_typ', 'a_tar', ]]
    # print info_df.head()
    result_df = info_df.fillna(0)
    result_df = pd.DataFrame(result_df).reindex()

    # 天下无双
    super_active_sql = '''
                        select t1.ds,t1.user_id,t1.server,t2.vip,'天下无双' as active_name, t3.score from (
                            select ds,user_id,reverse(substr(reverse(user_id), 8)) as server
                            from parse_actionlog
                            WHERE ds = '{date}'
                            and a_typ like '%super_active%'
                            group by ds,user_id,reverse(substr(reverse(user_id), 8))
                        )t1 left outer join(
                          select user_id,vip from mid_info_all where ds='{date}' group by user_id,vip
                          )t2 on t1.user_id=t2.user_id
                          left outer join(
                            select user_id,sum(coin_num) as score from raw_spendlog where ds='{date}' group by user_id
                            )t3 on t1.user_id=t3.user_id
                        group by t1.ds,t1.user_id,t1.server,t2.vip,t3.score
    '''.format(date=date)
    super_active_df = hql_to_df(super_active_sql).fillna(0)
    # print super_active_df.head()

    # 上古神兵-团购
    group_buy_sql = '''
        select t1.ds,t1.user_id,t1.server,t2.vip,'上古神兵' as active_name, t3.score from (
                            select ds,user_id,reverse(substr(reverse(user_id), 8)) as server
                            from parse_actionlog
                            WHERE ds = '{date}'
                            and a_typ like '%group_active_buy%'
                            group by ds,user_id,reverse(substr(reverse(user_id), 8))
                        )t1 left outer join(
                          select user_id,vip from mid_info_all where ds='{date}' group by user_id,vip
                          )t2 on t1.user_id=t2.user_id
                          left outer join(
                            select user_id,sum(coin_num) as score from raw_spendlog where ds='{date}'  and  goods_type = 'group_buy.group_active_buy'  group by user_id
                            )t3 on t1.user_id=t3.user_id
        group by t1.ds,t1.user_id,t1.server,t2.vip,t3.score
    '''.format(date=date)
    group_buy_df = hql_to_df(group_buy_sql).fillna(0)


    if result_df.__len__() != 0 or super_active_df.__len__() != 0 or group_buy_df.__len__() != 0:
        if result_df.__len__() == 0:
            data = DataFrame({'ds': [],
                              'user_id': [],
                              'server': [],
                              'vip': [],
                              'active_name': [],
                              'score': [],
                              })
        else:
            ds_list, user_id_list, service_id_list, vip_list, active_name_list, core_list = [
            ], [], [], [], [], []
            for i in range(len(result_df)):
                ds = result_df.iloc[i, 0]
                user_id = result_df.iloc[i, 1]
                service_id = result_df.iloc[i, 2]
                vip = result_df.iloc[i, 3]
                a_typ = result_df.iloc[i, 4]
                tar = result_df.iloc[i, 5]
                core = 0
                if a_typ.endswith('do_limit_gacha'):
                    if 'gacha_id' in tar:
                        active_name = '限时神将'
                        tar = eval(tar)
                        times = int(tar['times'])
                        if times == 10:
                            core = 110
                        else:
                            core = int(times) * 10

                        ds_list.append(ds)
                        user_id_list.append(user_id)
                        service_id_list.append(service_id)
                        vip_list.append(vip)
                        active_name_list.append(active_name)
                        core_list.append(int(core))

            data = DataFrame({'ds': ds_list,
                              'user_id': user_id_list,
                              'server': service_id_list,
                              'vip': vip_list,
                              'active_name': active_name_list,
                              'score': core_list,
                              })
            # print '----'
            # print data.head(5)

        data = pd.concat([data,super_active_df,group_buy_df])
        # print data

        result_df = data.groupby(['ds', 'user_id', 'server', 'vip', 'active_name']).agg(
            {'score': lambda g: g.sum()}).reset_index()
        # print result_df.head()
        result_df['rank'] = result_df.groupby(
            ['server', 'active_name', ])['score'].rank(method='first', ascending=False)
        result_df = result_df.sort_values(
            ['server', 'active_name', 'rank'], ascending=True)
        result_df = result_df[result_df['rank'] <= 10]
        # print result_df.head()

        money_spend_sql = '''
                  select ds,user_id,sum(order_money) as money
                  from raw_paylog
                  WHERE ds = '{date}'
                  and platform_2 !="admin_test"
                  group by ds,user_id
            '''.format(date=date)
        money_spend_df = hql_to_df(money_spend_sql)
        money_spend_df = money_spend_df.fillna(0)
        money_spend_df['money'] = money_spend_df['money'].astype("int")

        coin_spendlog = '''
                select ds,user_id,sum(coin_num) as coin_spend from raw_spendlog where ds='{date}' group by ds,user_id
            '''.format(date=date)
        coin_spendlog_df = hql_to_df(coin_spendlog)

        yes_vip_sql = '''
                 select user_id,name,vip as yes_vip,coin as yes_coin from mid_info_all where ds='{date}' group by user_id,name,vip,yes_coin
        '''.format(date=ds_add(date, -1))
        yes_vip_df = hql_to_df(yes_vip_sql)

        result_df = result_df.merge(
            coin_spendlog_df, on=[
                'ds', 'user_id'], how='left')
        result_df = result_df.merge(
            money_spend_df, on=[
                'ds', 'user_id'], how='left')
        result_df = result_df.merge(yes_vip_df, on=['user_id', ], how='left')
        result_df = result_df.merge(server_df, on=['ds', 'server'], how='left')
        result_df['ds'] = pd.to_datetime(result_df.ds)
        result_df = DataFrame(result_df).fillna(0).drop_duplicates()
        # print result_df.head()

        result_df = result_df.rename(
            columns={
                'act_ds': 'days',
                'ds': 'active_date',
                'coin_spend': 'spend',
                'money': 'today_pay',
            })
        result_df = result_df[['rank',
                               'user_id',
                               'name',
                               'score',
                               'spend',
                               'today_pay',
                               'vip',
                               'yes_vip',
                               'yes_coin',
                               'server',
                               'active_name',
                               'active_date',
                               'days',
                               ]]
        # print result_df

        table = 'country_ks_active_info'
        print '更新MySQL表 --->', table
        update_sql = 'delete from {0} where active_date = "{1}"'.format(
            table, format_date(date))
        update_mysql(table, result_df, update_sql, 'godvs')


if __name__ == '__main__':
    settings_dev.set_env('sanguo_ks')
    date = (datetime.date.today() - datetime.timedelta(days=1)
            ).isoformat().replace('-', '')
    print date
    # for date in date_range('20170625', '20170703'):
    #     print date
    active_info(date)
    print 'end'