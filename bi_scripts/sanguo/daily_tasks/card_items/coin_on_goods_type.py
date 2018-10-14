#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-10-30 下午2:56
@Author  : Andy
@File    : coin_on_goods_type.py
@Software: PyCharm
Description :
'''
from utils import hql_to_df, date_range, ds_add
import settings_dev
import pandas as pd
from sanguo.cfg import zichong_uids


def goods_coin(ds, reg_date):
    settings_dev.set_env('sanguo_ks')
    goods_sql = '''
          select t2.user_id,reverse(substr(reverse(t2.user_id), 8)) as service_id,t3.vip,t4.is_not_reg,t2.goods_type,sum(t2.coin_num) as coin_num
          from (
                select user_id,goods_type,sum(coin_num) as coin_num
                from raw_spendlog
                where ds="{ds}"
                group by user_id,goods_type
                )t2
                left outer join(
                  select user_id,max(vip) as vip
                  from raw_info
                  where ds="{ds}"
                  group by user_id
                )t3 on t2.user_id=t3.user_id
                left outer join(
                  select user_id,
                  case when substr(reg_time,1,10) = "{reg_date}" then 1 else 0 end as is_not_reg
                  from raw_info
                  where ds = "{ds}"
                  group by user_id,is_not_reg
                )t4 on t2.user_id = t4.user_id
          where reverse(substr(reverse(t2.user_id), 8)) in ('m1','m2')
          group by  t2.user_id,t3.vip,t4.is_not_reg,t2.goods_type
          '''.format(ds=ds, reg_date=reg_date)
    print goods_sql
    goods_type_df = hql_to_df(goods_sql)
    goods_type_df.fillna(0)
    print goods_type_df
    # 总消耗
    goods_all_df = goods_type_df.groupby(['service_id','goods_type']).agg(
        {'coin_num': lambda g: g.sum()}).reset_index()
    goods_all_df = goods_all_df.rename(columns={'coin_num': 'coin_num_all', })
    # print goods_all_df
    # 免费用户
    free_df = goods_type_df[goods_type_df['vip'] == 0]
    goods_free_df = free_df.groupby(['service_id','goods_type']).agg(
        {'coin_num': lambda g: g.sum()}).reset_index()
    goods_free_df = goods_free_df.rename(
        columns={'coin_num': 'coin_num_free', })
    # print goods_free_df
    # 付费用户
    pay_df = goods_type_df[goods_type_df['vip'] != 0]
    goods_pay_df = pay_df.groupby(['service_id','goods_type']).agg(
        {'coin_num': lambda g: g.sum()}).reset_index()
    goods_pay_df = goods_pay_df.rename(columns={'coin_num': 'coin_num_pay', })
    # print goods_pay_df
    # 新增注册消耗
    reg_df = goods_type_df[goods_type_df['is_not_reg'] == 1]
    goods_reg_df = reg_df.groupby(['service_id','goods_type']).agg(
        {'coin_num': lambda g: g.sum()}).reset_index()
    goods_reg_df = goods_reg_df.rename(columns={'coin_num': 'coin_num_reg', })
    # print goods_reg_df

    # 装配
    result = goods_all_df.merge(goods_free_df, on=['service_id','goods_type', ], how='left')
    #result = pd.DataFrame(result).fillna(0)
    result = result.merge(goods_pay_df, on=['service_id','goods_type', ], how='left')
    #result = pd.DataFrame(result).fillna(0)
    result = result.merge(goods_reg_df, on=['service_id','goods_type', ], how='left')
    result = pd.DataFrame(result).fillna(0)
    # print "结果",result
    result = pd.DataFrame(result)
    result['ds'] = ds
    result.to_excel("/home/kaiqigu/机甲无双_金山_%s_按goods_type得钻石消耗_免费用户消耗_付费用户消耗_新增消耗.xlsx" % ds,index=False)
    return result
if __name__ == '__main__':
    ds_list = [
        '20160419',
        '20160420',
        '20160421',
        '20160422',
        '20160423',
        '20160424',
        '20160425',
        '20160426',
        '20160427',
        '20160428']
    ds_reg_list = [
        '2016-04-19',
        '2016-04-20',
        '2016-04-21',
        '2016-04-22',
        '2016-04-23',
        '2016-04-24',
        '2016-04-25',
        '2016-04-26',
        '2016-04-27',
        '2016-04-28']
    ds_len = len(ds_list)
    fianl = pd.DataFrame()
    for i in range(0, ds_len):
        res = goods_coin(ds_list[i], ds_reg_list[i])
        fianl.append(res)
        print fianl
    fianl.to_excel('/home/kaiqigu/机甲无双_金山_04月19到04月28_按goods_type得钻石消耗_免费用户消耗_付费用户消耗_新增消耗.xlsx',index=False)
    print "end"
