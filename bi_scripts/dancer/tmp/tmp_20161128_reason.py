#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 主要数据点：

武娘国内版本

取数据：uid，账号，注册时间，等级，vip等级，战力，最后活跃时间，最后通关副本，最后有效动作，钻石消耗接口数量

流失用户：等级、注册时间、玩家最后有效动作、vip区间、副本通关、钻石消耗行为

未流失用户：注册时间、活跃度高的功能&活动，等级区间，vip区间
Database    : dancer_tw
'''
import settings_dev
import pandas as pd
from utils import hql_to_df
def tmp_20161128_reason(list):

    # 取info的信息
    info_sql = '''
        select
            user_id, account, max(reg_time) as reg_time, max(vip) as vip, max(level) as level, max(combat) as combat, max(ds) as ds, max(act_time) as act_time
        from
            parse_info
        where
            ds >= '20161103'
        group by user_id, account
    '''
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head(10)

    # 取action的信息
    # action_sql = '''
    #     select t1.user_id, t1.last_time, t2.a_tar, t3.a_typ from(
    #         select user_id, max(log_t) as last_time from parse_actionlog where ds>='20161103' group by user_id
    #         ) t1
    #         left join (
    #         select user_id, a_tar from (select user_id, a_tar, log_t, row_number() over(partition by user_id order by log_t desc) as rn from parse_actionlog where ds>='20161103' and a_typ='forward.forward') a1 where a1.rn=1
    #         ) t2 on t1.user_id = t2.user_id
    #         left join (
    #         select user_id, a_typ from (select user_id, a_typ, log_t, row_number() over(partition by user_id order by log_t desc) as rn from parse_actionlog where ds>='20161103' and a_typ in {list})a2 where a2.rn=1
    #         ) t3 on t1.user_id = t3.user_id
    #     '''.format(list=list)
    # action_sql = '''
    #     select user_id, a_tar from (select user_id, a_tar, log_t, row_number() over(partition by user_id order by log_t desc) as rn from parse_actionlog where ds>='20161103' and a_typ='forward.forward') a1 where a1.rn=1
    # '''.format(list=list)
    # print action_sql
    # raw_action_df = hql_to_df(action_sql)
    # # raw_action_df = raw_action_df.fillna("{'chapter':0,'stage_step':0}")
    # print raw_action_df.head(10)
    # user_id_list, last_time_list, chapter_list, step_list, a_typ_list = [], [], [], [], []
    # for i in range(len(raw_action_df)):
    #     user_id_list.append(raw_action_df.iloc[i, 0])
    #     # last_time_list.append(raw_action_df.iloc[i, 1])
    #     # a_typ_list.append(raw_action_df.iloc[i, 3])
    #     # try :
    #     tar = raw_action_df.iloc[i, 1]
    #     tar = eval(tar)
    #     chapter_list.append(tar['chapter'])
    #     step_list.append(tar['stage_step'])
        # except :
        #     pass
    # # action_df = pd.DataFrame({'user_id': user_id_list, 'last_time': last_time_list, 'chapter': chapter_list, 'stage_step': step_list, 'a_typ': a_typ_list})
    # action_df = pd.DataFrame(
    #         {'user_id': user_id_list, 'chapter': chapter_list, 'stage_step': step_list})
    # print action_df.head(10)

    # 取spend的信息
    spend_sql = '''
        select user_id, count(distinct goods_type), sum(coin_num) from raw_spendlog where ds>='20161103' group by user_id
    '''
    print spend_sql
    spend_df = hql_to_df(spend_sql)
    print spend_df.head(10)

    # all1 = info_df.merge(spend_df, on='user_id', how='left')

    all = info_df.merge(spend_df, on='user_id', how='left')
    print all.head(10)

    # all['liushi'] = all['act_time'] - all['reg_time']

    # info_df['liushi'] = info_df['act_time'] - info_df['reg_time']

    return all


if __name__ == '__main__':
    settings_dev.set_env('dancer_pub')
    list = ('')
    result = tmp_20161128_reason(list)
    result.to_excel('/home/kaiqigu/Documents/liushifenxi_spendsum.xlsx')
