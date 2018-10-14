#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-12 下午5:37
@Author  : Andy 
@File    : coin_on_goods_type_on_ds.py
@Software: PyCharm
Description :
'''

from utils import hql_to_df, date_range, ds_add
import settings_dev
import pandas as pd
from sanguo.cfg import zichong_uids


def goods_coin(ds, reg_date):
    settings_dev.set_env('sanguo_kr')
    goods_sql = '''
          select t2.ds,t2.user_id,reverse(substr(reverse(t2.user_id), 8)) as service_id,t3.vip,t4.is_not_reg,t2.goods_type,sum(t2.coin_num) as coin_num
          from (
                select ds,user_id,goods_type,sum(coin_num) as coin_num
                from raw_spendlog
                where ds="{ds}"
                group by ds,user_id,goods_type
                )t2
                left outer join(
                  select ds,user_id,max(vip) as vip
                  from raw_info
                  where ds="{ds}"
                  group by ds,user_id
                )t3 on (t2.user_id=t3.user_id and t2.ds=t3.ds)
                left outer join(
                  select ds,user_id,
                  case when substr(reg_time,1,10) = "{reg_date}" then 1 else 0 end as is_not_reg
                  from raw_info
                  where ds = "{ds}"
                  group by ds,user_id,is_not_reg
                )t4 on (t2.user_id = t4.user_id and t2.ds=t4.ds)
          where reverse(substr(reverse(t2.user_id), 8)) in ('kr3','kr4')
          group by  t2.ds,t2.user_id,t3.vip,t4.is_not_reg,t2.goods_type
          '''.format(ds=ds, reg_date=reg_date)
    print goods_sql
    goods_type_df = hql_to_df(goods_sql)
    goods_type_df.fillna(0)
    print goods_type_df.head(10)
    # 总消耗
    goods_all_df = goods_type_df.groupby(['ds','service_id','goods_type']).agg(
        {'coin_num': lambda g: g.sum()}).reset_index()
    goods_all_df = goods_all_df.rename(columns={'coin_num': 'coin_num_all', })
    print goods_all_df
    # 免费用户
    free_df = goods_type_df[goods_type_df['vip'] == 0]
    goods_free_df = free_df.groupby(['ds','service_id','goods_type']).agg(
        {'coin_num': lambda g: g.sum()}).reset_index()
    goods_free_df = goods_free_df.rename(
        columns={'coin_num': 'coin_num_free', })
    print goods_free_df
    # 付费用户
    pay_df = goods_type_df[goods_type_df['vip'] != 0]
    goods_pay_df = pay_df.groupby(['ds','service_id','goods_type']).agg(
        {'coin_num': lambda g: g.sum()}).reset_index()
    goods_pay_df = goods_pay_df.rename(columns={'coin_num': 'coin_num_pay', })
    print goods_pay_df
    # 新增注册消耗
    reg_df = goods_type_df[goods_type_df['is_not_reg'] == 1]
    goods_reg_df = reg_df.groupby(['ds','service_id','goods_type']).agg(
        {'coin_num': lambda g: g.sum()}).reset_index()
    goods_reg_df = goods_reg_df.rename(columns={'coin_num': 'coin_num_reg', })
    # print goods_reg_df

    # 装配
    result = goods_all_df.merge(goods_free_df, on=['ds','service_id','goods_type', ], how='left')
    #result = pd.DataFrame(result).fillna(0)
    result = result.merge(goods_pay_df, on=['ds','service_id','goods_type', ], how='left')
    #result = pd.DataFrame(result).fillna(0)
    result = result.merge(goods_reg_df, on=['ds','service_id','goods_type', ], how='left')
    result = pd.DataFrame(result).fillna(0)
    # print "结果",result
    result = pd.DataFrame(result)
    #result['ds'] = ds
    #result.to_excel("/home/kaiqigu/机甲无双_韩国_%s_按goods_type得钻石消耗_免费用户消耗_付费用户消耗_新增消耗.xlsx" % ds,index=False)
    return result
if __name__ == '__main__':
    ds_list = [
        '20161207',
        '20161208',
        '20161209',
        '20161210',
        '20161211']
    ds_reg_list = [
        '2016-12-07',
        '2016-12-08',
        '2016-12-09',
        '2016-12-10',
        '2016-12-11']
    ds_len = len(ds_list)
    fianl = []
    for i in range(0, ds_len):
        res = goods_coin(ds_list[i], ds_reg_list[i])
        a=ds_list[i]
        res.to_excel('/home/kaiqigu/机甲无双_韩国__%s_按goods_type得钻石消耗_免费用户消耗_付费用户消耗_新增消耗.xlsx'%a, index=False)
        fianl.append(res)
        print fianl
    fianl.to_excel('/home/kaiqigu/机甲无双_韩国_按goods_type得钻石消耗_免费用户消耗_付费用户消耗_新增消耗.xlsx',index=False)
    print "end"
