#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 付费流失用户生命周期
'''
import settings_dev
from utils import hql_to_df, update_mysql, ds_add


def dis_common_pay_loss(date):
    lost_date = ds_add(date, -7)
    lost_date = lost_date[:4] + '-' + lost_date[4:6] + '-' + lost_date[-2:]
    info_sql = '''
    SELECT account,
           to_date(min(reg_time)) reg_time,
           to_date(max(act_time)) act_time,
           max(vip_exp) vip_exp,
           datediff(max(act_time), min(reg_time)) life_day
    FROM mid_info_all
    WHERE ds = '{0}' and reg_time != '1970-01-01 08:00:00' and act_time != '1970-01-01 08:00:00'
    GROUP BY account
        '''.format(date)
    info_df = hql_to_df(info_sql)

    info_df['is_pay'] = info_df['vip_exp'] > 0
    info_df['is_lost'] = info_df['act_time'] < lost_date
    info_df['ds'] = date

    print info_df

    # 更新原始数据表
    table = 'dis_common_pay_loss_raw'
    del_sql = 'drop table {0}'.format(table)
    update_mysql(table, info_df, del_sql)

    info_agg_df = info_df.groupby('reg_time').agg({
        'account': lambda c: c.count(),
        'is_pay': lambda c: c.sum(),
        'is_lost': lambda c: c.sum(),
        'life_day': {
            'mean': lambda c: c.mean(),
            'median': lambda c: c.median(),
        }
    }).reset_index()
    info_agg_df.columns = [' '.join(col).strip()
                           for col in info_agg_df.columns.values]
    columns_rename_dic = {
        u'reg_time': 'ds',
        u'life_day median': 'life_day_median',
        u'life_day mean': 'life_day_mean',
        u'account <lambda>': 'reg_num',
        u'is_pay <lambda>': 'pay_num',
        u'is_lost <lambda>': 'loss_num',
    }
    info_agg_df.rename(columns=columns_rename_dic, inplace=True)

    # 更新MySQL表——每日统计
    table = 'dis_common_pay_loss'
    del_sql = 'drop table {0}'.format(table)
    update_mysql(table, info_agg_df, del_sql)


if __name__ == '__main__':
    settings_dev.set_env('metal_test')
    date = '20160611'
    dis_common_pay_loss(date)
