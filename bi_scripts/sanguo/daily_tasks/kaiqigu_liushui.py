#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-7-3 下午2:04
@Author  : Andy 
@File    : kaiqigu_liushui.py
@Software: PyCharm
Description :   官网包每月的流水
'''

import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range


def dis_kaiqigu_pay_detail(date):
    month_start = date[0:6] + '01'  # 月初肯定是1号 所以 直接替换 就可以
    monthRange = calendar.monthrange(int(date[0:3]), int(date[4:6]))  # 得到本月的天数
    month_end = date[0:6] + str(monthRange[1])
    print date, '月初日期为：', month_start, '月末日期为：', month_end

    kaiqigu_pay_sql = '''
            select ds,order_id,order_money,order_time,platform_2
            from raw_paylog
            where ds >='{month_start}'
            and ds<='{month_end}'
            and (order_id like '{pay_way_a}' or order_id like'{pay_way_b}' or order_id like'{pay_way_c}')
            and platform_2 != 'admin_test'
            order by ds,order_id,order_time,platform_2
        '''.format(**{'month_start': month_start, 'month_end': month_end,
                      'pay_way_a': 'unionpay%', 'pay_way_b': 'weixin%', 'pay_way_c': 'alipay%'})
    print kaiqigu_pay_sql
    kaiqigu_pay_df = hql_to_df(kaiqigu_pay_sql)

    return kaiqigu_pay_df

if __name__ == '__main__':
    #dis_kaiqigu_pay_detail('20170102')
    for platform in ['sanguo_ks',]:
        settings_dev.set_env(platform)
        result = dis_kaiqigu_pay_detail('20170902')
        result.to_excel(r'/Users/kaiqigu/Documents/Sanguo/机甲无双-金山服-官网包流水-_20171009.xlsx', index=False)
    print "end"
