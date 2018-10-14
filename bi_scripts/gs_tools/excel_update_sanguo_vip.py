#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Author      : Dong JunShuang
Description : 根据Excel更新godvs的VIP用户信息数据
Time        : 2017。06.07
illustration:
"""
import pandas as pd
# from utils import hql_to_df
from utils import update_mysql
# import settings_dev
from utils import sql_to_df

if __name__ == '__main__':
    date = '20170606'
    table = 'country_ks_vipuserinfo'
    old_sql = "select * from {0}".format(table)
    # 获取godvs中的老数据
    old_df = sql_to_df(old_sql, 'godvs')
    # 获取需求数据
    # use_old_df = old_df[['role_id', 'role_name', 'vip_username',
    #                      'vip_birthday', 'vip_qq', 'vip_telephone',
    #                      'vip_wechat', 'vip_last_time_conn']]
    info_df = pd.read_excel(r'/Users/kaiqigu/Downloads/country_vip_info.xls')
    result = old_df.merge(info_df, on='role_id', how='left')

    # update_mysql('aa', result_df, del_sql, 'godvs')
    def get_new_data():
        for _, row in result.iterrows():
            if row.vip_birthday:
                vip_birthday = row.vip_birthday
            else:
                vip_birthday = row.birthday
            if row.vip_qq:
                vip_qq = row.vip_qq
            else:
                vip_qq = row.qq
            if row.vip_telephone:
                vip_telephone = row.vip_telephone
            else:
                vip_telephone = row.phone
            yield [row.vip_username, vip_birthday, vip_qq, vip_telephone,
                   row.vip_wechat, row.vip_last_time_conn, row.role_id,
                   row.role_name, row.level, row.vip_level, row.act_time]
    # 生成卡牌的DataFrame
    column = ['vip_username', 'vip_birthday', 'vip_qq', 'vip_telephone',
              'vip_wechat', 'vip_last_time_conn', 'role_id', 'role_name',
              'level', 'vip_level', 'act_time']
    result_df = pd.DataFrame(get_new_data(), columns=column)
    result_df = result_df.drop_duplicates('role_id')
    # del result_df['role_name']
    # result_df.to_excel('/Users/kaiqigu/Documents/Excel/aa.xlsx')
    del_sql = "delete from {0}".format(table)
    update_mysql(table, result_df, del_sql, 'godvs')
