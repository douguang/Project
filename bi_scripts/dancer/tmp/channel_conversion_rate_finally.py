#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-7 下午12:04
@Author  : Andy 
@File    : channel_conversion_rate_finally.py
@Software: PyCharm
Description :  渠道转换率
'''

import pandas as pd
def txt_reduce(file_from_1,file_from_2,file_goto):
    # device	time	plat	日期
    vt_df_1 = pd.read_excel(file_from_1)
    print vt_df_1.head(2)
    vt_df_2 = pd.read_excel(file_from_2)
    print vt_df_2.head(2)
    result = vt_df_1.merge(vt_df_2, on=['ds','device',], how='left')
    result = pd.DataFrame(result).fillna(0)
    print result.head(2)
    print "=========="

    final_df = result.groupby(['ds', 'plat', ]).agg({'account': lambda g:g.count()}).reset_index()
    final_df = final_df.rename(columns={'account': 'no_account', })
    print pd.DataFrame(final_df).head(2)
    final_df_2 = result.groupby(['ds', 'plat',]).agg({'account': lambda g: g.sum()}).reset_index()
    final_df = final_df.rename(columns={'account': 'you_account', })
    print pd.DataFrame(final_df_2).head(2)
    final_df_3 = result.groupby(['ds', 'plat',]).agg({'account': lambda g: g.sum() / g.count()}).reset_index()
    final_df_3 = final_df_3.rename(columns={'account': 'rate', })
    print pd.DataFrame(final_df_3).head(2)

    result = final_df.merge(final_df_2, on=['ds','plat', ], how='left')
    result = result.merge(final_df_3, on=['ds','plat', ], how='left')
    result = pd.DataFrame(result).fillna(0)

    pd.DataFrame(result).to_excel(file_goto,index=False)

    print 'end'

if __name__ == '__main__':
    file_from_1 = "/home/kaiqigu/桌面/all_channel_conversion_rate_去重.xlsx"
    file_from_2 = "/home/kaiqigu/桌面/hive中的设备数据(20-06).xlsx"
    file_goto = '/home/kaiqigu/桌面/转换率(1208).xlsx'
    txt_reduce(file_from_1,file_from_2,file_goto)

