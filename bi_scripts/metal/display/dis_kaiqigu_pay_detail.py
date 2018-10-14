#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-2-8 上午10:39
@Author  : Andy 
@File    : dis_kaiqigu_pay_detail.py
@Software: PyCharm
Description :   [官网包支付订单明细]  支付宝/微信/银联   alipay/unionpay/weixin
'''

import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range

def dis_kaiqigu_pay_detail(date):
    month_start = date[0:6] + '01'  # 月初肯定是1号 所以 直接替换 就可以
    monthRange = calendar.monthrange(int(date[0:3]), int(date[4:6]))  # 得到本月的天数
    month_end = date[0:6] + str(monthRange[1])
    print date, '月初日期为：', month_start, '月末日期为：', month_end

    kaiqigu_pay_sql='''
        select ds,order_id,order_money,order_time
        from raw_paylog
        where ds >='{month_start}'
        and ds<='{month_end}'
        and (order_id like '{pay_way_a}' or order_id like'{pay_way_b}' or order_id like'{pay_way_c}')
        order by ds,order_id,order_time
    '''.format(**{'month_start': month_start,'month_end': month_end,
              'pay_way_a': 'unionpay%','pay_way_b': 'weixin%','pay_way_c':'alipay%'})
    print kaiqigu_pay_sql
    kaiqigu_pay_df = hql_to_df(kaiqigu_pay_sql)
    #print kaiqigu_pay_df
    table = 'dis_kaiqigu_pay_detail'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, kaiqigu_pay_df, del_sql)

if __name__ == '__main__':
    #dis_kaiqigu_pay_detail('20170102')
    for platform in ['metal_test',]:
        settings_dev.set_env(platform)
        dis_kaiqigu_pay_detail('20170102')
    print "end"

