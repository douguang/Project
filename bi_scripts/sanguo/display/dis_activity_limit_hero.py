#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-29 下午2:48
@Author  : Andy 
@File    : dis_activity_limit_hero.py
@Software: PyCharm
Description :  限时神将
server_card_gacha.do_limit_gacha	{u'channel_id': u'', u'identifier': u'356156073414120', u'devicename': u'SM-G9350', u'gacha_id': u'2003', u'times': u'10'}
card_gacha.do_limit_gacha	{u'devicename': u'iPadMini1G(A1432)', u'times': u'1', u'channel_id': u'', u'identifier': u'31A14E7B-6C82-48B0-8E53-0686B1B47484', u'gacha_id': u'2001'}

'''

import pandas as pd
from pandas import DataFrame
from utils import get_active_conf, hql_to_df, ds_short, update_mysql, date_range
import settings_dev
import  time
def dis_activity_limit_hero(date):
    version, act_start_time, act_end_time = get_active_conf('limit_hero_score',
                                                            date)
    if version != '':
        act_start_short = ds_short(act_start_time)
        act_end_short = ds_short(act_end_time)

        dis_limit_hero(act_start_short,act_end_short,)
    else:
        print '{0} 没有限时神将活动'.format(date)

def dis_limit_hero(act_start_short,act_end_short,):
    table = 'dis_activity_limit_hero'
    print table
    info_sql = '''
        select user_id,reverse(substr(reverse(user_id), 8)) as server,max(vip_level) as vip,a_tar,log_t
        from parse_actionlog
        WHERE ds >= '{act_start_short}'
        AND ds<='{act_end_short}'
        and a_typ like  '%do_limit_gacha%'
        and a_tar like '%gacha_id%'
        group by user_id,reverse(substr(reverse(user_id), 8)),a_tar,log_t
    '''.format(act_start_short=act_start_short,
               act_end_short=act_end_short,)

    info_df = hql_to_df(info_sql)
    info_df = info_df[['user_id', 'server', 'vip', 'a_tar', ]]
    result_df = info_df.fillna(0)
    result_df['ds'] = act_start_short
    result_df = pd.DataFrame(result_df).reindex()

    if result_df.__len__() != 0:

        ds_list, user_id_list, service_id_list,vip_list, gacha_id_list,a_tar_list = [], [], [], [], [],[]
        for i in range(len(result_df)):

            user_id = result_df.iloc[i, 0]
            service_id = result_df.iloc[i, 1]
            vip = result_df.iloc[i, 2]
            tar = result_df.iloc[i, 3]
            ds = result_df.iloc[i, 4]

            tar = eval(tar)
            gacha_id = tar['gacha_id']
            times = tar['times']
            ds_list.append(ds)
            user_id_list.append(user_id)
            service_id_list.append(service_id)
            vip_list.append(vip)
            gacha_id_list.append(gacha_id)
            a_tar_list.append(int(times))

        data = DataFrame({'ds': ds_list,
                          'user_id': user_id_list,
                          'server': service_id_list,
                          'vip': vip_list,
                          'gacha_id': gacha_id_list,
                          'times': a_tar_list,
                          })
        #print data
        mid_a_df = data[data['times'] == 10]
        mid_a_df['core']=110
        mid_b_df = data[data['times'] == 1]
        mid_b_df['core'] = 10
        mid_result=[]
        mid_result.append(mid_a_df)
        mid_result.append(mid_b_df)
        data = pd.concat(mid_result).reindex()

        result1 = data.groupby(['ds', 'user_id','server','vip',]).agg(
            {'times': lambda g: g.sum()}).reset_index()

        result2 = data.groupby(['ds', 'user_id', 'server', 'vip', ]).agg(
            {'core': lambda g: g.sum()}).reset_index()
        result = DataFrame(result1).merge(result2,on=['ds', 'user_id', 'server', 'vip',],how='left')

        result = DataFrame(result).reindex()
        result['ds'] = result['ds'].astype("str")
        result['user_id'] = result['user_id'].astype("str")
        result['server'] = result['server'].astype("str")
        result['vip'] = result['vip'].astype("int")
        result['times'] = result['times'].astype("int")
        result['core'] = result['core'].astype("int")
        result_df = result.fillna(0)
        result_df = DataFrame(result_df)

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
                                    select t1.user_id,t1.server,t2.spend,t3.server_spend
                                    from(
                                      select user_id,reverse(substr(reverse(user_id), 8)) as server
                                      from parse_actionlog
                                      WHERE ds >= '{act_start_short}'
                                      AND ds<='{act_end_short}'
                                      group by user_id,reverse(substr(reverse(user_id), 8))
                                      )t1
                                      left outer join(
                                        select user_id,sum(coin_num) as spend
                                        from raw_spendlog
                                        WHERE ds >= '{act_start_short}'
                                        AND ds<='{act_end_short}'
                                        group by user_id
                                        )t2 on (t1.user_id=t2.user_id)
                                        left outer join(
                                          select reverse(substr(reverse(user_id), 8)) as server,sum(coin_num) as server_spend
                                          from raw_spendlog
                                          WHERE ds >= '{act_start_short}'
                                          AND ds<='{act_end_short}'
                                          group by reverse(substr(reverse(user_id), 8))
                                          )t3 on (t1.server=t3.server)
                                     group by t1.user_id,t1.server,t2.spend,t3.server_spend
                                '''.format(act_start_short=act_start_short,
                                           act_end_short=act_end_short,)

        spend_df = hql_to_df(spend_sql)
        spend_df = spend_df.fillna(0)

        result_df = result_df.merge(money_spend_df,on=['user_id'],how='left')
        result_df = result_df.merge(spend_df,on=['user_id','server'],how='left')
        result_df =DataFrame(result_df).fillna(0)

        result_df['money'] = result_df['money'].astype("int")
        result_df['spend'] = result_df['spend'].astype("int")
        result_df['server_spend'] = result_df['server_spend'].astype("int")

        # 更新MySQL表
        del_sql = 'delete from {0} where ds="{1}"'.format(table, act_start_short)
        update_mysql(table, result_df, del_sql)

if __name__ == '__main__':
    for platform in ['sanguo_tl',]:
          settings_dev.set_env(platform)
          for date in date_range('20170301','20170305'):
                result = dis_activity_limit_hero(date)
    print "end"



