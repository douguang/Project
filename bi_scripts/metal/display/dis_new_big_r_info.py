#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 新增鲸鱼用户分析
'''
import settings_dev
from utils import hql_to_df, update_mysql, ds_add


def dis_new_big_r_info(date):
    table = 'dis_new_big_r_info'
    date_before_seven = ds_add(date, -6)
    # 首次达到鲸鱼用户的判断标准
    big_r_pay_rmb = 500

    # TODO：充值和消费做uid每日汇总，然后改写该sql
    new_big_r_info_sql = """
    select '{date}' as ds,
           t1.user_id as uid,
           pay_rmb_sum,
           charge_coin_sum,
           coin,
           coalesce(spend_coin_sum, 0) as spend_coin_sum,
           reg_time,
           first_pay_date,
           last_pay_date,
           vip,
           level,
           d7_pay_date_num
    from
    (
        select user_id,
               sum(order_money) as pay_rmb_sum,
               sum(case when ds<'{date}' then order_money else 0 end) as pay_rmb_sum_before_yestoday,
               -- sum(case when ds='{date}' then order_money else 0 end) as pay_rmb_sum_today,
               sum(gift_coin)+sum(order_coin) as charge_coin_sum,
               max(ds) as last_pay_date,
               min(ds) as first_pay_date,
               count(distinct case when ds >= '{date_before_seven}' and ds <= '{date}' then ds else null end) as d7_pay_date_num
        from raw_paylog
        where ds <= '{date}'
        group by user_id
        having pay_rmb_sum_before_yestoday < {big_r_pay_rmb} and pay_rmb_sum >= {big_r_pay_rmb}
    ) t1
    -- 合并消费信息
    left outer join
    (
        select user_id, sum(coin_num) as spend_coin_sum
        from raw_spendlog
        where ds <= '{date}'
        group by user_id
    ) t2 on (t1.user_id = t2.user_id)
    -- 合并用户快照的一些信息
    left outer join
    (
        select user_id, vip, level, reg_time, coin
        from mid_info_all
        where ds= '{date}'
    ) t3 on (t1.user_id = t3.user_id)
    """.format(**{
        'date': date,
        'date_before_seven': date_before_seven,
        'big_r_pay_rmb': big_r_pay_rmb
    })

    print new_big_r_info_sql
    new_big_r_info_df = hql_to_df(new_big_r_info_sql)
    print new_big_r_info_df.head()
    # new_big_r_info_df.to_excel('/tmp/new_big_r_{platform}_{date}.xlsx'.format(date=date, platform=settings_dev.platform), index=False)

    # 更新MySQL表
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, new_big_r_info_df, del_sql)


if __name__ == '__main__':
    settings_dev.set_env('metal_test')
    dis_new_big_r_info('20160426')
    # settings_dev.set_env('sanguo_tx')
    # dis_new_big_r_info('20160421')
