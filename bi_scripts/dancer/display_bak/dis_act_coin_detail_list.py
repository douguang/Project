#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 武娘 三日活跃且钻石存量存量异常的用户详细列表
create_date : 2016.10.12
'''
import settings_dev
import pandas as pd
from utils import update_mysql,ds_add,hqls_to_dfs, hql_to_df, get_config
from dancer.cfg import zichong_uids

zichong_list = str(tuple(zichong_uids))

def dis_act_coin_detail_list(date):

    #用户信息
    user_sql = '''
    SELECT server ,
           user_id ,
           vip ,
           LEVEL ,
           coin ,
           reg_time ,
           act_time ,
           coin_num
    FROM
      ( SELECT reverse(substring(reverse(user_id), 8)) AS server,
               user_id,
               vip,
               LEVEL,
               coin,
               reg_time,
               act_time,
               coin_num
       FROM
         ( SELECT user_id,
                  vip,
                  LEVEL,
                  free_coin as coin,
                  reg_time,
                  act_time,
                  CASE WHEN free_coin >=50000 THEN 's50000+' WHEN free_coin >=5000
                       AND free_coin <=10000 THEN 's5000_10000' WHEN free_coin >=10001
                       AND free_coin <=15000 THEN 's10001_15000' WHEN free_coin >=15001
                       AND free_coin <=20000 THEN 's15001_20000' WHEN free_coin >=20001
                       AND free_coin <=30000 THEN 's20001_30000' WHEN free_coin >=30001
                       AND free_coin <=50000 THEN 's30001_50000' ELSE 'None' END AS coin_num
          FROM mid_info_all
          WHERE ds = '{date}' and user_id not in {zichong_list}) a
       WHERE coin_num !='None' ) t1 LEFT semi
    JOIN
      ( SELECT user_id
       FROM ext_activeuser
       WHERE ds <= '{date}'
         AND ds >= '{date_in_3days}' ) t2 ON t1.user_id = t2.user_id
    '''.format(**{'date':date,'date_in_3days':ds_add(date,-2), 'zichong_list':zichong_list})              #导入动态信息,date字符串调用ds_add实现减法

    # 支付信息
    pay_sql = '''
    SELECT user_id ,
           sum(order_money) AS sum_money ,
           max(order_time) AS pay_time
    FROM
      ( SELECT user_id ,
               order_money ,
               order_time
       FROM raw_paylog
       WHERE ds <= '{date}'
         AND ds >= '{date_in_3days}'
         AND platform_2 <> 'admin_test'
         AND order_id not like '%testktwwn%')a
    GROUP BY user_id
    '''.format(**{'date':date,'date_in_3days':ds_add(date,-2),})              #导入动态信息

    # 打印检查sql语句
    print user_sql
    print pay_sql

    user_df,pay_df = hqls_to_dfs([user_sql,pay_sql])         #调用hqls函数执行sql语句查询得到dataframe结果
    result = user_df.merge(pay_df,on='user_id',how='left')         #调用pandas的merge函数合并dataframe
    result = result.fillna(0)                               #调用pandas的fillna函数将nan补0
    result['ds'] = date                                     #添加索引ds
    columns = ['ds', 'coin_num', 'server', 'user_id', 'vip', 'sum_money',
               'coin', 'level', 'reg_time', 'act_time', 'pay_time']
    result = result[columns]       #添加字段名


    # 更新MySQL表
    table = 'dis_act_coin_detail_list'                      #设置标题
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)      #delete语句
    update_mysql(table, result, del_sql)                    #更新表

    return result

#测试文件，被调用时不会执行
if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')                           #台服
    date = '20161010'
    result = dis_act_coin_detail_list(date)
    print result
    print type(result)
    result.to_excel('/home/kaiqigu/Documents/ceshi.xlsx')
