#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 部分玩家钻石存量异常
'''
import datetime
import settings
from utils import hql_to_df, timestamp_to_string, date_range
import pandas as pd
import os

settings.set_env('superhero_bi')
results = []

date = '20160426'

# for date in date_range('20160301', '20160502'):
#     spendlog_sql = '''
#     select user_id, subtime, goods_type, coin_1st, coin_2nd
#     from raw_spendlog
#     where ds = '{date}'
#     order by user_id, subtime
#     '''.format(date=date)
#     spendlog_df = hql_to_df(spendlog_sql)
#     spendlog_df
#     print spendlog_df

#     lastrow = None
#     for _, row in spendlog_df.iterrows():
#         if lastrow is not None and lastrow.user_id == row.user_id and row.coin_1st - lastrow.coin_1st > 40000:
#             results.append(lastrow.tolist())
#             results.append(row.tolist())
#         lastrow = row
#     df = pd.DataFrame(results)
#     df.to_excel('/tmp/coin_yichang_user_%s.xlsx' % date, index=False)

# payinfo_sql = '''
# select ds, user_id, sum(order_money) as pay_rmb, sum(order_coin+gift_coin) as charge_coin
# from superhero_bi.raw_paylog
# where ds >= '20160301' and ds <= '20160501'
# group by ds, user_id
# '''
# payinfo_df = hql_to_df(payinfo_sql)
# payinfo_df.to_excel('/tmp/payinfo.xlsx')

# vip_sql = '''
# select uid, zuanshi, vip_level
# from mid_info_all
# where ds = '20160429'
# '''
# vip_df = hql_to_df(vip_sql)
# vip_df.to_excel('/tmp/uid_vip.xlsx')

# df = pd.read_excel('/tmp/coin_yichang_user_20160426.xlsx')
# payinfo_df = pd.read_excel('/tmp/payinfo.xlsx')
# vip_df = pd.read_excel('/tmp/uid_vip.xlsx').rename(columns={'uid': 'user_id'})
# df.columns = ['user_id', 'subtime', 'action', 'pre_coin', 'post_coin']
# df['ds'] = df['subtime'].map(lambda s: s[:10].replace('-', ''))
# payinfo_df['ds'] = payinfo_df.ds.map(str)
# df['ds'] = df.ds.map(str)
# df_final = df.merge(vip_df, on='user_id').merge(payinfo_df, on=['ds', 'user_id'], how='left')
# df_final.to_excel('/tmp/coin_yichang_final.xlsx')

start = '20160114'
end = '20160131'
path = '/home/data/superhero/action_log/action_log'
# path = '/home/data/superhero_qiku/log_temp/qq_action_log'

last_uid_action_coin = {}   # uid: [tp, action, post_coin, pre_coin]
for d in date_range(start, end):
    print d
    if True:#not os.path.exists('%s_%s_sorted' % (path, d)):
        command = 'sort %s_%s > %s_%s_sorted' % (path, d, path, d)
        os.system(command)
        print command, 'done!'
    with open('%s_%s_sorted' % (path, d)) as f, open('/home/data/http_path/uids_yichang_coin_%s.csv' % d, 'w') as f_out:
        for line in f:
            try:
                l = line.strip().split('\t')
                tp = float(l[0])
                uid = l[3]
                pre_coin = l[8]
                post_coin = float(l[26])
                if post_coin < 0:
                    continue
                action = l[40]
                if uid in last_uid_action_coin and post_coin - last_uid_action_coin[uid][2] > 40000:
                    latest = last_uid_action_coin[uid]
                    print timestamp_to_string(tp), uid, post_coin, latest[2]
                    f_out.write('%s,%s,%s,%s,%s\n' % (timestamp_to_string(latest[0]), uid, latest[1], latest[2], latest[3]))
                    f_out.write('%s,%s,%s,%s,%s\n' % (timestamp_to_string(tp), uid, action, post_coin, pre_coin))
                last_uid_action_coin[uid] = (tp, action, post_coin, pre_coin)
            except:
                # raise
                # f_error.write(line)
                pass


# g464926899_act_sql = '''
# select ds,
#        uid,
#        action,
#        t.timestamp,
#        pre_coin,
#        post_coin
# from superhero_bi.raw_action_log t
# where ds = '20160405' and uid = 'g464926899'
# '''
# g464926899_act_df = hql_to_df(g464926899_act_sql, 'hive')
# g464926899_act_df['datetime'] = g464926899_act_df['t.timestamp'].map(timestamp_to_string)
# g464926899_act_df.to_excel('/tmp/g464926899_act_log.xlsx', index=False)

# hql_temp = '''
# select ds,
#        uid,
#        action,
#        t.timestamp,
#        pre_coin,
#        post_coin
# from superhero_bi.raw_action_log t
# where -- ds >= '20151201'
#       ds >= '{start}' and ds < '{end}'
#       and rc = '0'
#       and uid in ('g464926899', 'g2106246265', 'g477272409', 'g426495462', 'g1011861804', 'g476380692', 'g425650882', 'g386883646')
# '''
# start_end_list = (
#     ('20151201', '20160101', '1512'),
#     ('20160101', '20160201', '1601'),
#     ('20160201', '20160301', '1602'),
#     ('20160301', '20160401', '1603'),
# )
# for start, end, name in start_end_list:
#     hql = hql_temp.format(start=start, end=end)
#     print start, end, name
#     print hql
#     df = hql_to_df(hql, 'hive')
#     df.to_excel('~/superhero_tmp_data/superhero_uid_action_coin_%s.xlsx' % name, index=False)

