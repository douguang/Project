#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: xiangzi.py 
@time: 18/3/6 下午8:11 
"""

import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range
import pandas as pd

def data_reduce():

    info_sql = '''
      select t1.uid,t1.act_time,t1.action,t1.zhongshendian,t1.rn from (
      select uid,act_time,action,zhongshendian,row_number() over( order by uid,act_time,action,zhongshendian) as rn  from raw_action_log where ds>='20180305' and action = 'item.use' and act_time>='2018-03-05 18:15:00' group by uid,act_time,action,zhongshendian
      )t1
      where t1.rn <=1000
    '''
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head()
    # info_df.to_excel(r'/Users/kaiqigu/Documents/Superhero/超英-越南-箱子数据-元数据_20180307.xlsx',index=False)

    user_id_list,act_time_list,action_list,demo_list,num_list = [],[],[],[],[]
    for _, row in info_df.iterrows():
        print '----------------'

        # print row
        uid = row.uid
        act_time = row.act_time
        action = row.action
        data_dic = dict()
        # if 'min_fate_sto15' not in row.zhongshendian:
        # if isinstance(dict(), action)
        try:
            data_dic = eval(row.zhongshendian)
        except Exception:
            print '=Error'
            print row
            pass


        # print data_dic
        # print type(data_dic)
        reward = data_dic.get('reward',{})
        # print type(reward)
        reward = dict(reward)
        # print reward
        # print type(reward)

        if reward != dict():
            for key,value in reward.iteritems():

                if key == 'grace':
                    num = reward.get('grace', '')
                    user_id_list.append(uid)
                    act_time_list.append(act_time)
                    action_list.append(action)
                    demo_list.append(key)
                    num_list.append(num)
                    # print 'grace'
                    # print key
                    # print num
                if key == 'food':
                    num = reward.get('food', '')
                    user_id_list.append(uid)
                    act_time_list.append(act_time)
                    action_list.append(action)
                    demo_list.append(key)
                    num_list.append(num)
                    # print 'food'
                    # print key
                    # print num

                if key == 'metal':
                    num = reward.get('metal', '')
                    user_id_list.append(uid)
                    act_time_list.append(act_time)
                    action_list.append(action)
                    demo_list.append(key)
                    num_list.append(num)
                    # print 'metal'
                    # print key
                    # print num

                if key == 'refine_stone':
                    num = reward.get('refine_stone', '')
                    user_id_list.append(uid)
                    act_time_list.append(act_time)
                    action_list.append(action)
                    demo_list.append(key)
                    num_list.append(num)
                    # print 'refine_stone'
                    # print key
                    # print num

                if key == 'small_forge_stone':
                    num = reward.get('small_forge_stone', '')
                    user_id_list.append(uid)
                    act_time_list.append(act_time)
                    action_list.append(action)
                    demo_list.append(key)
                    num_list.append(num)
                    # print 'small_forge_stone'
                    # print key
                    # print num


                if key == 'item':
                    value = reward.get('item', '')
                    # print '-------'
                    # print value
                    # print 'item_type:',type(value)
                    u_words = set(value)
                    for wo in u_words:
                        num = value.count(wo)
                        user_id_list.append(uid)
                        act_time_list.append(act_time)
                        action_list.append(action)
                        demo_list.append(wo)
                        num_list.append(num)
                        # print wo,num
                        # print 'item'
                        # print wo
                        # print num


                if key == 'cards':
                    value = reward.get('cards', '')
                    print 'item_type:', type(value)
                    for a in value:
                        user_id_list.append(uid)
                        act_time_list.append(act_time)
                        action_list.append(action)
                        demo_list.append(key)
                        num_list.append(a)
                        # print 'cards'
                        # print key
                        # print a

                print 'end'



    result_df = pd.DataFrame({'user_id': user_id_list, 'act_time': act_time_list, 'action': action_list, 'demo': demo_list, 'num': num_list})


    return result_df

if __name__ == '__main__':

    for platform in ['superhero_vt',]:
        settings_dev.set_env(platform)
        result = data_reduce()
        # result.to_csv(r'/Users/kaiqigu/Documents/Superhero/超英-越南-箱子数据_20180307.csv')
        result.to_excel(r'/Users/kaiqigu/Documents/Superhero/超英-越南-箱子数据_20180307.xlsx', index=False)
    print "end"