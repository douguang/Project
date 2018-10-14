#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 每日钻石消费
'''
from utils import hqls_to_dfs, ds_add, update_mysql
import settings
import pandas as pd


if __name__ == '__main__':
    settings.set_env('superhero_bi')
#     date = '20160628'
# def dis_day_coin_spend(date):
    # sum_dic = {ds_add(date, -1): 'yes_coin', date: 'day_coin'}
    sum_sql = '''
    SELECT ds,
           sum(zuanshi) sum_coin
    FROM mid_info_all
    WHERE ds >= '20160707' and ds <= '20160710'
    and substr(uid,1,1) = 'g'
    GROUP BY ds
    '''
    spend_sql = '''
    SELECT ds,
           sum(coin_num) spend_coin
    FROM raw_spendlog
    WHERE ds >= '20160707' and ds <= '20160710'
    and substr(uid,1,1) = 'g'
    GROUP BY ds
    '''
    pay_sql = '''
    SELECT ds,
           sum(order_coin) pay_coin
    FROM raw_paylog
    WHERE ds >= '20160707' and ds <= '20160710'
    and substr(uid,1,1) = 'g'
    GROUP BY ds
    '''
    sum_df, spend_df, pay_df = hqls_to_dfs([sum_sql, spend_sql, pay_sql])
    result_df = (sum_df.merge(spend_df,on=['ds'],how='outer')
                        .merge(pay_df,on=['ds'],how='outer'))
    result_df = result_df.sort_values(by='ds')
    # sum_result_df = (sum_df.pivot_table(
    #     'sum_coin', 'server', 'ds').reset_index().rename(columns=sum_dic))
    # if date == settings.start_date.strftime('%Y%m%d'):
    #     sum_result_df['yes_coin'] = 0
    # result_df = (sum_result_df.merge(spend_df, on='server', how='outer')
    #              .merge(pay_df, on='server', how='outer'))
    # result_df['add_coin'] = result_df['day_coin'] - result_df['yes_coin']
    # result_df['ds'] = date

    # column = ['ds', 'server', 'add_coin', 'pay_coin', 'day_coin', 'spend_coin']
    # result_df = result_df[column]
    # rename_dic = {'add_coin':'new_coin','pay_coin':'pay_get_coin','day_coin':'coin_save',
    #             'spend_coin':'coin_spend'}
    # result_df = result_df.rename(columns=rename_dic)
    # result_df = result_df.groupby('ds').sum().reset_index()
    # print result_df
    # return result_df

    # # 更新MySQL表
    # table = 'dis_day_coin_spend'
    # del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    # update_mysql(table, result_df, del_sql)


# if __name__ == '__main__':
#     settings.set_env('superhero_vt')
    # date = '20160629'
    # result_df = dis_day_coin_spend(date)
    # while date <= '20160707':
    #     date = ds_add(date,1)
    #     print date
    #     data = dis_day_coin_spend(date)
    #     result_df = pd.concat([result_df,data])
    # result_df.to_excel('/Users/kaiqigu/Downloads/Excel/zuanshi.xlsx')


