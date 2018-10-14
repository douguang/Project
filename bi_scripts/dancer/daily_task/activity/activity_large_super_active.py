#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  华山之巅
@software: PyCharm 
@file: activity_large_super_active.py 
@time: 18/2/24 下午2:51 
"""

import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range
import json
import pandas as pd


def data_reduce():

    data_sql = '''
        select ds,user_id,server,account,a_typ,a_rst,log_t from parse_actionlog where ds>='20180206' and a_typ like 'large_super_active%' 
    '''
    print data_sql
    data_df = hql_to_df(data_sql)
    print data_df.head()

    def card_evo_lines():
        for _, row in data_df.iterrows():

                # print [row.ds, row.server, row.reg_time,row.dau, delta]
                rst = eval(row.a_rst)
                print rst.__len__
                if rst.__len__ == 0:
                    print
                    print [row.ds, row.account, row.user_id, row.server, row.a_typ, row.log_t, '', '', '','', ]
                    yield [row.ds, row.account, row.user_id, row.server, row.a_typ, row.log_t, '', '', '','', ]
                else:
                    for dic_demo in rst:

                        diff = dic_demo['diff']
                        after = dic_demo['after']
                        obj = dic_demo['obj']
                        before = dic_demo['before']

                        print [row.ds,row.account,row.user_id,row.server,row.a_typ,row.log_t,obj,before,after,diff,]
                        yield [row.ds,row.account,row.user_id,row.server,row.a_typ,row.log_t,obj,before,after,diff,]

    result_df = pd.DataFrame(card_evo_lines(), columns=['ds', 'account', 'user_id','server','a_typ','log_t', 'obj', 'before','after','diff',])
    # result_df = info_df[info_df.user_id!=info_df.user_id]
    print result_df.head()
    # result_df = result_df.groupby(['ds',]).agg({
    #     'account': lambda g: g.nunique(),
    # }).reset_index().rename(columns={'account': 'num',})

    return result_df


    # res = data_df.merge(info_df, on=['user_id', ], how='left')
    # return data_df

if __name__ == '__main__':
    #dis_kaiqigu_pay_detail('20170102')
    for platform in ['dancer_cgame',]:
        settings_dev.set_env(platform)
        result = data_reduce()
        result.to_excel(r'/Users/kaiqigu/Documents/Dancer/武娘-cagme-华山之巅_20180224.xlsx', index=False)
    print "end"