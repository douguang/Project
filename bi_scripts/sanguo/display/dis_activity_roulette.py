#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-28 下午5:41
@Author  : Andy 
@File    : dis_activity_roulette.py
@Software: PyCharm
Description : 蒸汽转盘    roulette.roulette
{u'channel_id': u'', u'identifier': u'com.mancala.jbsg01.tw', u'times': u'10'}
'''

from pandas import DataFrame
from utils import get_active_conf, hql_to_df, ds_short, update_mysql, date_range
import settings_dev
import time


def dis_activity_roulette(date):
    version, act_start_time, act_end_time = get_active_conf('roulette',
                                                            date)
    if version != '':
        act_start_short = ds_short(act_start_time)
        act_end_short = ds_short(act_end_time)
        print version, act_start_time, act_end_time
        dis_roulettle(act_start_short, act_end_short,)
    else:
        print '{0} 没有蒸汽转盘活动'.format(date)


def dis_roulettle(act_start_short, act_end_short,):
    table = 'dis_activity_roulette'
    print table
    info_sql = '''
        select user_id,reverse(substr(reverse(user_id), 8)) as server,max(vip_level) as vip, a_tar,log_t
        from parse_actionlog
        WHERE ds >= '{act_start_short}'
        AND ds<='{act_end_short}'
        and a_typ = 'roulette.roulette' and a_tar like "%time%"
        group by user_id,reverse(substr(reverse(user_id), 8)), a_tar,log_t
    '''.format(act_start_short=act_start_short,
               act_end_short=act_end_short,)
    print info_sql
    info_df = hql_to_df(info_sql)
    info_df = info_df[['user_id','server','vip','a_tar',]]
    print info_df.head(3)
    result_df = info_df.fillna(0)

    if result_df.empty is False:

        ds_list, user_id_list, service_id_list, vip_list, a_tar_list = [], [], [], [], []
        for i in range(len(result_df)):
            user_id = result_df.iloc[i, 0]
            service_id = result_df.iloc[i, 1]
            vip = result_df.iloc[i, 2]
            tar = result_df.iloc[i, 3]

            tar = eval(tar)
            times = tar['times']
            ds_list.append(act_start_short)
            user_id_list.append(user_id)
            service_id_list.append(service_id)
            vip_list.append(vip)
            a_tar_list.append(int(times))

        data = DataFrame({'ds': ds_list,
                          'user_id': user_id_list,
                          'server': service_id_list,
                          'vip': vip_list,
                          'times': a_tar_list,
                          })
        result = data.groupby(
            ['ds', 'user_id', 'server', 'vip']).sum().times.reset_index()

        result['core'] = result['times'] * 10
        result = DataFrame(result).reindex()
        result['ds'] = result['ds'].astype("str")
        result['user_id'] = result['user_id'].astype("str")
        result['server'] = result['server'].astype("str")
        result['vip'] = result['vip'].astype("int")
        result['times'] = result['times'].astype("int")
        result['core'] = result['core'].astype("int")
        result_df = result

        result_df = result_df.sort_values(by=['core'], ascending=False)
        result_df['rank'] = range(1, (len(result_df) + 1))
        result_df['rank'] = result_df['rank'].astype("int")
        result_df = result_df[result_df['rank'] <= 500]

        money_spend_sql = '''
                      select user_id,sum(order_money) as money
                      from raw_paylog
                      WHERE ds >= '{act_start_short}'
                      AND ds<='{act_end_short}'
                      and platform_2 !="admin_test"
                      group by user_id
                '''.format(act_start_short=act_start_short,
                           act_end_short=act_end_short,)

        money_spend_df = hql_to_df(money_spend_sql)
        money_spend_df = money_spend_df.fillna(0)

        spend_sql = '''
                    SELECT t1.user_id,
                           t1.server,
                           t2.spend,
                           t3.server_spend from
                      ( SELECT user_id,reverse(substr(reverse(user_id), 8)) AS server
                       FROM parse_actionlog
                       WHERE ds >= '{act_start_short}'
                         AND ds<='{act_end_short}'
                       GROUP BY user_id,reverse(substr(reverse(user_id), 8)) )t1
                    LEFT OUTER JOIN
                      ( SELECT user_id,
                               sum(coin_num) AS spend
                       FROM raw_spendlog
                       WHERE ds >= '{act_start_short}'
                         AND ds<='{act_end_short}'
                       GROUP BY user_id )t2 ON (t1.user_id=t2.user_id)
                    LEFT OUTER JOIN
                      ( SELECT reverse(substr(reverse(user_id), 8)) AS server,
                               sum(coin_num) AS server_spend
                       FROM raw_spendlog
                       WHERE ds >= '{act_start_short}'
                         AND ds<='{act_end_short}'
                       GROUP BY reverse(substr(reverse(user_id), 8)) )t3 ON (t1.server=t3.server)
                    GROUP BY t1.user_id,
                             t1.server,
                             t2.spend,
                             t3.server_spend
                '''.format(act_start_short=act_start_short,
                           act_end_short=act_end_short,)

        spend_df = hql_to_df(spend_sql)
        spend_df = spend_df.fillna(0)

        result_df = result_df.merge(money_spend_df, on=['user_id'], how='left')
        result_df = result_df.merge(
            spend_df, on=['user_id', 'server'], how='left')
        result_df = DataFrame(result_df).fillna(0)

        result_df['money'] = result_df['money'].astype("int")
        result_df['spend'] = result_df['spend'].astype("int")
        result_df['server_spend'] = result_df['server_spend'].astype("int")

        # 更新MySQL表
        del_sql = 'delete from {0} where ds="{1}"'.format(table, act_start_short)
        update_mysql(table, result_df, del_sql)

if __name__ == '__main__':
    for platform in [ 'sanguo_tl',]:
        settings_dev.set_env(platform)
        for date in date_range('20170228', '20170228'):
            result = dis_activity_roulette(date)
    print "end"
