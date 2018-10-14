#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-11-30 下午12:25
@Author  : Andy
@File    : dis_plat_ltv.py
@Software: PyCharm
Description :  分渠道LTV
'''
from utils import hql_to_df, ds_add, update_mysql
from utils import hql_to_df, date_range, ds_add
import settings_dev
import pandas as pd
import time

def plat_ltv(date):
    ltv_ds = ds_add(date, -59)
    # ltv_days = [3, 7, 14, 30, 60]
    # for a in ltv_days
    for da in date_range(ltv_ds, date):
        if da >= '20161110':
            dis_plat_ltv(da)

def dis_plat_ltv(date):
    #注册用户account,platform
    before_ds = ds_add(date,-1)
    after_ds = ds_add(date,+1)
    print date,before_ds,after_ds
    reg_user_sql='''
        select user_id,account,platform
        from parse_actionlog
        where ds>='{before_ds}'
        and ds<='{after_ds}'
        and user_id in(
          select user_id
          from parse_info
          where ds>='{before_ds}'
          and ds<='{after_ds}'
          and regexp_replace(substr(reg_time,1,10),'-','') = '{date}'
          )
        group by user_id,account,platform
    '''.format(before_ds=before_ds,after_ds=after_ds,date=date)
    print reg_user_sql
    reg_user_df = hql_to_df(reg_user_sql)
    reg_user_df['ds'] = date
    reg_user_df = pd.DataFrame(reg_user_df).reindex()
    print reg_user_df.head(3)

    #注册用户付费
    ltv_days = [3, 7, 14, 30, 60]
    max_ltv_ds=max(ltv_days)
    pay_user_sql = '''
        select t1.ds,t2.account,t1.user_id,t1.order_money
        from(
          select ds,user_id,sum(order_money) as order_money
          from raw_paylog
          where ds>="{date}"
          and ds<='{max_ltvs_ds}'
          and platform_2<>'admin_test' AND order_id not like '%testktwwn%'
          group by ds,user_id
        )t1
        left outer join(
          select user_id,account
          from parse_actionlog
          where ds>="{date}"
          and ds<='{max_ltvs_ds}'
          group by user_id,account
          )t2 on t1.user_id = t2.user_id
        group by t1.ds,t2.account,t1.user_id,t1.order_money
    '''.format(max_ltvs_ds=ds_add(date,max_ltv_ds),date=date)
    print pay_user_sql
    pay_user_df = hql_to_df(pay_user_sql)
    print pay_user_df.head(3)

    # 计算LTV
    # 计算每个渠道的新增
    plat_reg_num_df = reg_user_df.groupby(['ds','platform',]).agg(
        {'account': lambda g: g.nunique()}).reset_index()
    plat_reg_num_df = plat_reg_num_df.rename(columns={'account': 'reg_user_num', })
    plat_reg_num_df = pd.DataFrame(plat_reg_num_df).reindex()

    #计算LTV天的总付费收入
    mid_reg_user_df = reg_user_df[['account','platform']]
    reg_plat_ltv_pay_df = pay_user_df.merge(mid_reg_user_df, on=['account', ], how='left')
    print reg_plat_ltv_pay_df.head(2)
    # reg_plat_ltv_pay_df = pd.DataFrame(reg_plat_ltv_pay_df).reindex()
    reg_plat_ltv_pay_df = pd.DataFrame(reg_plat_ltv_pay_df).dropna().reindex()
    print reg_plat_ltv_pay_df.head(2)
    #筛选注册用户的account消费数据
    result = plat_reg_num_df
    print "==============================================================================================="
    print result.head(3)
    # 分两部分：一部分是新增为0，一部分是新增不为0
    # 一部分是新增为0

    # result_0 = result[result['reg_user_num'] == 0]
    # result_0 = result[['ds', 'platform',]]
    final_df = plat_reg_num_df[['ds', 'platform',]]
    # result_0 = pd.DataFrame(result_0).reindex()
    # print "新增注册人数为0",result_0.head(3)
    # print '最后一行',len(result_0)
    # if len(result_0) != 0:
    #     for ltv_day in ltv_days:
    #         ltv_day_pay = '%s_ltv_pay' % ltv_day
    #
    #         max_ds_pay = reg_plat_ltv_pay_df.ds.max()
    #
    #         ltv_num = '%s_ltv' % ltv_day
    #         ltv_ds = ds_add(date,ltv_day-1)
    #         pay_user_num = '%s_pay_user_num' % ltv_day
    #         #
    #         # if ltv_day_pay <= max_ds_pay:
    #         #     #先使这个期间的付费记为0
    #         #     #计算这个期间的新增付费人数付费
    #         #     # 先取这个期间的付费
    #         #     ltv_range_pay_df = \
    #         #     reg_plat_ltv_pay_df.loc[(reg_plat_ltv_pay_df.ds >= date) & (reg_plat_ltv_pay_df.ds <= ltv_ds)][
    #         #         ['platform', 'order_money']].groupby('platform').sum().reset_index()
    #         #     reg_plat_ltv_pay_df['order_money'] = 0
    #         #     ltv_range_pay_df = ltv_range_pay_df.rename(columns={'order_money': ltv_day_pay, })
    #         #     print pd.DataFrame(ltv_range_pay_df).head(10)
    #         # # 计算这个期间的新增付费人数付费
    #         #
    #         # ltv_range_pay_num_df = \
    #         # reg_plat_ltv_pay_df.loc[(reg_plat_ltv_pay_df.ds >= date) & (reg_plat_ltv_pay_df.ds <= ltv_ds)][
    #         #     ['platform', 'account']].groupby('platform').agg({'account': lambda g: g.nunique()}).reset_index()
    #         # ltv_range_pay_num_df = ltv_range_pay_num_df.rename(columns={'account': pay_user_num, })
    #         # ltv_range_pay_num_df = pd.DataFrame(ltv_range_pay_num_df).reindex()
    #         # print ltv_range_pay_num_df.head(3)
    #         #
    #         # result_0 = result_0.merge(ltv_range_pay_num_df, on=['platform', ], how='left')
    #         # # result = result.merge(ltv_range_pay_df, on=['platform', ], how='left')
    #         #
    #         # result_0[ltv_num] = 0
    #         # print pd.DataFrame(result_0).head(2)
    #         #
    #         # else:
    #         #计算这个期间的新增付费人数付费
    #         if ltv_day_pay <= max_ds_pay:
    #             ltv_range_pay_num_df = reg_plat_ltv_pay_df.loc[(reg_plat_ltv_pay_df.ds >= date) & (reg_plat_ltv_pay_df.ds <= ltv_ds)][
    #                 ['platform', 'account']].groupby('platform').agg({'account': lambda g: g.nunique()}).reset_index()
    #             ltv_range_pay_num_df = ltv_range_pay_num_df.rename(columns={'account':pay_user_num, })
    #             ltv_range_pay_num_df = pd.DataFrame(ltv_range_pay_num_df).reindex()
    #             print ltv_range_pay_num_df.head(3)
    #
    #             result_0 = result_0.merge(ltv_range_pay_num_df, on=['platform', ], how='left')
    #         else:
    #             ltv_range_pay_num_df = \
    #             reg_plat_ltv_pay_df.loc[(reg_plat_ltv_pay_df.ds >= date) & (reg_plat_ltv_pay_df.ds <= max_ds_pay)][
    #                 ['platform', 'account']].groupby('platform').agg({'account': lambda g: g.nunique()}).reset_index()
    #             ltv_range_pay_num_df = ltv_range_pay_num_df.rename(columns={'account': pay_user_num, })
    #             ltv_range_pay_num_df = pd.DataFrame(ltv_range_pay_num_df).reindex()
    #             print ltv_range_pay_num_df.head(3)
    #
    #             result_0 = result_0.merge(ltv_range_pay_num_df, on=['platform', ], how='left')
    #
    #         result_0[ltv_day_pay]=0
    #         result_0[ltv_num] = 0
    #         print pd.DataFrame(result_0).head(2)

    result = result[result['reg_user_num'] != 0]
    result = pd.DataFrame(result).reindex()
    #一部分是新增不为0
    #ltv_days
    for ltv_day in ltv_days:
        ltv_day_pay = '%s_ltv_pay' % ltv_day
        #max_ds_pay = pay_user_df.ds.max()
        max_ds_pay = time.strftime('%Y%m%d',time.localtime(time.time()))
        print "max_ds_pay",max_ds_pay
        ltv_num = '%s_ltv' % ltv_day
        ltv_ds = ds_add(date, ltv_day - 1)
        pay_user_num = '%s_pay_user_num' % ltv_day

        if ltv_ds <= max_ds_pay:
            #先取这个期间的付费
            ltv_range_pay_df = reg_plat_ltv_pay_df.loc[(reg_plat_ltv_pay_df.ds >= date) & (reg_plat_ltv_pay_df.ds <= ltv_ds)][
                ['platform', 'order_money']].groupby('platform').sum().reset_index()
            ltv_range_pay_df = ltv_range_pay_df.rename(columns={'order_money': ltv_day_pay, })
            print 'LTV期间的付费',pd.DataFrame(ltv_range_pay_df).head(10)
            #计算这个期间的新增付费人数付费
            print "000000000000000000000000000000000000000000000000000"

            ltv_range_pay_num_df = reg_plat_ltv_pay_df.loc[(reg_plat_ltv_pay_df.ds >= date) & (reg_plat_ltv_pay_df.ds <= ltv_ds)][
                ['platform', 'account']].groupby('platform').agg({'account': lambda g: g.nunique()}).reset_index()
            print "ltv_range_pay_num_df",reg_plat_ltv_pay_df.head(3)
            print "ltv_range_pay_num_df",ltv_range_pay_num_df.head(3)
            ltv_range_pay_num_df = ltv_range_pay_num_df.rename(columns={'account':pay_user_num, })
            ltv_range_pay_num_df = pd.DataFrame(ltv_range_pay_num_df).reindex()
            ltv_range_pay_num_df.fillna(0.0)
            print 'LTV期间的付费人数人数',ltv_range_pay_num_df.head(3)

            result = result.merge(ltv_range_pay_num_df, on=['platform', ], how='left')
            result = result.merge(ltv_range_pay_df, on=['platform', ], how='left')

            result[ltv_num] = result[ltv_day_pay] / result['reg_user_num']
            print '无新增为0的注册用户的结果',pd.DataFrame(result).head(2)
        else:
            # 计算这个期间的新增付费人数付费
            ltv_range_pay_num_df = \
            reg_plat_ltv_pay_df.loc[(reg_plat_ltv_pay_df.ds >= date) & (reg_plat_ltv_pay_df.ds <= max_ds_pay)][
                ['platform', 'account']].groupby('platform').agg({'account': lambda g: g.nunique()}).reset_index()
            pay_user_num = '%s_pay_user_num' % ltv_day
            ltv_range_pay_num_df['account'] = 0
            ltv_range_pay_num_df = ltv_range_pay_num_df.rename(columns={'account': pay_user_num, })
            ltv_range_pay_num_df = pd.DataFrame(ltv_range_pay_num_df).reindex()
            print 'LTV期间的付费人数人数', ltv_range_pay_num_df.head(3)
            result = result.merge(ltv_range_pay_num_df, on=['platform', ], how='left')
            result = pd.DataFrame(result).fillna(0.0)
            result[ltv_day_pay] = 0
            result[ltv_num] = 0
            print pd.DataFrame(result).head(2)

    print result.head(10)
    final_df = pd.DataFrame(final_df).merge(result, on=['ds','platform', ], how='left')
    final_df = pd.DataFrame(final_df).fillna(0.0)
    #result = result.merge(result_0, on=['ds','platform', ], how=inner)
    #result = pd.DataFrame(result).fillna(0.0).reindex()
    print final_df.head(10)
    final_df.to_excel('/home/kaiqigu/LTV_hanpeng.xlsx')
    return final_df
    # 更新MySQL
    table = 'dis_plat_ltv'
    print date, table

    # 修改列名的排列顺序
    # column_list = ['ds',
    #                'platform',
    #                'all_num',
    #                'have_user_num',
    #                'attend_sum',
    #                'attend_rate',
    #                'org_attend_player_num',
    #                'red_attend_player_num',
    #                'org_all_num',
    #                'red_all_num'] + ['red_%r' % r for r in range(red_max_step + 1)]
    # for i in (red_max_step + 1, 20):
    #     key_in = 'red_%r' % i
    #     result[key_in] = 0
    #     result.reindex()
    #
    # result[column_list] = result[column_list].astype(int)
    # print result


    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table,result, del_sql)

if __name__ == '__main__':
    print time.time()
    for platform in ['dancer_pub', ]:
        settings_dev.set_env(platform)
        # for date in date_range('20161124', '20161124'):
        #     print date
        #     plat_ltv(date)
        result = plat_ltv('20161205')

    print "end"

