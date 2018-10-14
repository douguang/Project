#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-7 下午5:20
@Author  : Andy
@File    : dis_days_coin_spend.py
@Software: PyCharm
Description : 机甲无双  钻石存量与消耗
'''

import settings_dev
from utils import hql_to_df, update_mysql, date_range
import pandas as pd

def dis_days_coin_spend(date):
    # table = 'dis_days_coin_spend'
    table = 'dis_day_coin_spend'
    print table,date
    server_vip_sql = '''
      select ds,reverse(substr(reverse(user_id), 8)) AS server,user_id,max(vip_level) as vip
      from parse_actionlog
      where ds="{date}"
      group by ds,user_id
    '''.format(**{
        'date': date,
    })
    server_vip_df = hql_to_df(server_vip_sql)
    server_vip_df = server_vip_df.fillna(0)
    print server_vip_df.head(3)

    free_get_coin_sql = '''
      select ds,user_id,sum(nvl(freemoney_diff,0)) as free_get_coin
      from parse_actionlog
      where ds="{date}"
      and freemoney_diff>0
      group by ds,user_id
    '''.format(**{
        'date': date,
    })

    free_get_coin_df = hql_to_df(free_get_coin_sql)
    free_get_coin_df = free_get_coin_df.fillna(0)
    print free_get_coin_df.head(3)

    # pay_get_coin_sql = '''
    #     select ds,user_id,sum(nvl(money_diff,0)) as pay_get_coin
    #     from parse_actionlog
    #     where ds="{date}"
    #     and money_diff>0
    #     group by ds,user_id
    # '''.format(**{
    #     'date': date,
    # })
    pay_get_coin_sql = '''
        select ds,user_id,sum(order_coin) as pay_get_coin
        from raw_paylog
        where ds="{date}"
        group by ds,user_id
    '''.format(**{
        'date': date,
    })

    pay_get_coin_df = hql_to_df(pay_get_coin_sql)
    pay_get_coin_df = pay_get_coin_df.fillna(0)
    print pay_get_coin_df.head(3)

    coin_save_sql = '''
        select ds,user_id,sum(coin) as coin_save
        from raw_info
        where ds="{date}"
        group by ds,user_id
    '''.format(**{
        'date': date,
    })
    coin_save_df = hql_to_df(coin_save_sql)
    coin_save_df = coin_save_df.fillna(0)
    print coin_save_df.head(3)

    spend_coin_sql = '''
        select ds,user_id,sum(coin_num) as coin_spend
        from raw_spendlog
        where ds="{date}"
        group by ds,user_id
    '''.format(**{
        'date': date,
    })
    spend_coin_df = hql_to_df(spend_coin_sql)
    spend_coin_df = spend_coin_df.fillna(0)
    print spend_coin_df.head(3)

    # ds server  vip  dau  new_coin  free_get_coin  pay_get_coin coin_save  coin_spend
    # server_vip_df  free_get_coin_df+pay_get_coin_df free_get_coin_df pay_get_coin_df coin_save_df spend_coin_df
    # 拼装
    result = server_vip_df.merge(
        free_get_coin_df, on=[
            'ds', 'user_id', ], how='left')
    result = result.merge(pay_get_coin_df, on=['ds', 'user_id', ], how='left')
    result = result.merge(coin_save_df, on=['ds', 'user_id', ], how='left')
    result = result.merge(spend_coin_df, on=['ds', 'user_id', ], how='left')
    result = pd.DataFrame(result).fillna(0)
    print '==========='
    print result.head(3)
    # 计算DAU 和 钻石新增
    result['new_coin'] = result['free_get_coin'] + result['pay_get_coin']
    print result.head(3)
    result = result[result['server'] != 0]
    print result.head(3)
    dau_df = result.groupby(['ds', 'server', 'vip', ]).agg(
        {'user_id': lambda g: g.nunique()},
    ).reset_index()
    dau_df = dau_df.rename(columns={'user_id': 'dau', })
    print 'dau_df'
    print dau_df
    result = result[['ds',
                     'server',
                     'vip',
                     'new_coin',
                     'free_get_coin',
                     'pay_get_coin',
                     'coin_save',
                     'coin_spend']].groupby(['ds',
                                             'server',
                                             'vip',
                                             ]).sum().reset_index()

    name_1 = pd.DataFrame(result).icol(0).name
    #print name_1
    if name_1 != 'ds':
        result = pd.DataFrame(columns=['ds', 'server','vip','new_coin','free_get_coin','pay_get_coin','coin_save','coin_spend'])
        #print result.head(2)

    name_2 = pd.DataFrame(dau_df).icol(1).name
    #print name_2
    if name_2 == 'dau':
        dau_df = pd.DataFrame(columns=['ds', 'server', 'vip', 'dau'])
        #print dau_df
        #print result.head(1)
    #     result = result.merge(dau_df, on=['ds', 'server', 'vip', ], how='outer')
    # else:
    #     print result.head(1)
    result = result.merge(dau_df, on=['ds', 'server', 'vip', ], how='left')
    result = result[['ds', 'server', 'vip', 'dau', 'new_coin',
                     'free_get_coin', 'pay_get_coin', 'coin_save', 'coin_spend']]
    result = pd.DataFrame(result).fillna(0)
    print pd.DataFrame(result)

    rename_dic = {'free_coin': 'free_get_coin',
                  'charge_coin': 'pay_get_coin',
                  'consume_coin': 'coin_spend',
                  'save_coin': 'coin_save',
                  'vip': 'vip_level'}
    result = result.rename(columns=rename_dic)

    # 更新MySQL
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result, del_sql)

if __name__ == '__main__':
    # for platform in ['sanguo_tw', 'sanguo_ks','sanguo_kr',]:
    #     settings_dev.set_env(platform)
    #     for date in date_range('20161020', '20161206'):
    #         dis_days_coin_spend(date)
    #
    # print "end"
    for platform in ['sanguo_tl',]:
        settings_dev.set_env(platform)
        for date in date_range('20170301', '20170315'):
            dis_days_coin_spend(date)
    print "end"



