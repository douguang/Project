#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 新服数据
'''
from utils import hqls_to_dfs, ds_add, update_mysql, hql_to_df, format_dates
import settings
import pandas as pd

keep_days = [2, 3, 7]

def dis_new_server(date):
    info_sql = '''
    SELECT reverse(substring(reverse(uid), 8)) AS server,
           substr(uid,1,1) as plat,
           uid,
           vip_level
    FROM raw_info
    WHERE ds = '{0}'
    '''.format(date)
    pay_sql = '''
    SELECT uid,
           reverse(substring(reverse(uid), 8)) AS server,
           substr(uid,1,1) AS plat,
           sum(order_money) sum_money
    FROM raw_paylog
    WHERE ds ='{0}'
    GROUP BY uid,
             reverse(substring(reverse(uid), 8)),
             substr(uid,1,1)
    '''.format(date)
    reg_sql = '''
    SELECT reverse(substring(reverse(uid), 8)) AS server,
           substr(uid,1,1) as plat,
           count(uid) reg_user_num
    FROM raw_info
    WHERE ds = '{0}'
    and regexp_replace(substr(create_time,1,10),'-','') = '{1}'
    group by reverse(substring(reverse(uid), 8)),substr(uid,1,1)
    '''.format(date,date)
    info_df, pay_df, reg_df = hqls_to_dfs([info_sql, pay_sql, reg_sql])

    # VIP活跃人数
    vip_df = info_df.loc[info_df.vip_level > 0]
    vip_result_df = vip_df.groupby(['server', 'plat']).count().uid.reset_index().rename(columns={'uid':'vip_num'})
    # 非VIP活跃人数
    vip0_df = info_df.loc[info_df.vip_level == 0]
    vip0result_df = vip0_df.groupby(['server', 'plat']).count().uid.reset_index().rename(columns={'uid':'vip0_num'})
    # 充值人数,充值金额
    pay_result_df = (pay_df.groupby(['server', 'plat']).agg({'uid': 'count',
                                                             'sum_money': 'sum'})
                     .reset_index().rename(columns={'uid': 'pay_num'}))
    # 充值6元的人数
    pay6_df = pay_df.groupby(['uid', 'server', 'plat']).sum().reset_index()
    pay6_df = pay6_df.loc[pay6_df.sum_money == 6]
    pay6_result_df = pay6_df.groupby(['server', 'plat']).count().uid.reset_index().rename(columns={'uid':'pay6_num'})

    result_df = (vip_result_df.merge(vip0result_df,on=['server', 'plat'],how='outer')
        .merge(pay_result_df,on=['server', 'plat'],how='outer')
        .merge(pay6_result_df,on=['server', 'plat'],how='outer')
        .merge(reg_df,on=['server', 'plat'],how='outer')
        .fillna(0)
    )
    result_df['ds'] = date
    result_df['act_num'] = result_df['vip_num'] + result_df['vip0_num']
    result_df['pay_rate'] = result_df['pay_num'] * 1.0 / result_df['act_num']
    result_df['arpu'] = result_df['sum_money'] * 1.0 / result_df['act_num']
    result_df['arppu'] = result_df['sum_money'] * 1.0 / result_df['pay_num']

    columns = ['ds','server','plat','vip_num','vip0_num','pay_num','sum_money','pay6_num','reg_user_num','act_num','pay_rate','arpu','arppu']
    result_df = result_df[columns]

    # pub、ios数据
    pub_result_df = result_df.loc[result_df.plat == 'g']
    ios_result_df = result_df.loc[result_df.plat == 'a']

    # 更新MySQL表(pub)
    table = 'dis_new_server'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, pub_result_df, del_sql,'superhero_pub')
    # 更新MySQL表(ios)
    table = 'dis_new_server'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, ios_result_df, del_sql,'superhero_ios')

    # 调用留存率
    dis_new_server_keep_rate(date)

# 留存率
def dis_new_server_keep_rate(date):
    '''在某一天执行'''
    server_start_date = settings.start_date.strftime('%Y%m%d')
    # 要抓取的注册日期
    reg_dates = filter(lambda d: d >= server_start_date, [ds_add(date, 1 - d) for d in keep_days])
    if not reg_dates:
        return
    # 要抓取的活跃日期
    act_dates = set()
    for reg_date in reg_dates:
        act_dates.add(reg_date)
        for keep_day in keep_days:
            act_dates.add(ds_add(reg_date, keep_day - 1))

    reg_sql = '''
    SELECT reg_ds,
           uid,
           reverse(substring(reverse(uid), 8)) AS server,
           substr(uid,1,1) AS plat
    FROM
      (SELECT ds AS reg_ds,
              uid
       FROM raw_reg
       WHERE ds IN {reg_dates})a LEFT semi
    JOIN
      (SELECT ds ,
              uid
       FROM raw_info
       WHERE ds IN {reg_dates})b ON a.reg_ds = b.ds
    AND a.uid = b.uid
    '''.format(reg_dates=format_dates(reg_dates))
    act_sql = '''
    SELECT ds AS act_ds,
           uid,
           reverse(substring(reverse(uid), 8)) AS server,
           substr(uid,1,1) AS plat
    FROM raw_act
    WHERE ds IN {act_dates}
    '''.format(act_dates=format_dates(act_dates))
    reg_df,act_df = hqls_to_dfs([reg_sql,act_sql])
    reg_df['reg'] = 1

    # 活跃用户表转职后合并到注册表后，将活跃日期变成列名
    act_df['act'] = 1
    reg_act_df = (act_df
                  .pivot_table('act', ['uid','server','plat'], 'act_ds')
                  .reset_index()
                  .merge(reg_df,on=['uid','server','plat'] ,how='right')
                  .reset_index()
                  )

    # 求每一个受影响的日期留存率，然后合并
    keep_rate_dfs = []
    for reg_date in reg_dates:
        act_dates = [ds_add(reg_date, keep_day - 1) for keep_day in keep_days]
        act_dates_dic = {ds_add(reg_date, keep_day - 1): 'd%d_keep' % keep_day for keep_day in keep_days}
        keep_df = (reg_act_df
                   .loc[reg_act_df.reg_ds == reg_date,
                        ['reg','server','plat'] + act_dates_dic.keys()]
                   .rename(columns=act_dates_dic)
                   .groupby(['server','plat'])
                   .sum()
                   .reset_index()
                   .fillna(0)
                   )
        for c in act_dates_dic.values():
            keep_df[c + 'rate'] = keep_df[c] / keep_df.reg
        keep_df['ds'] = reg_date
        keep_rate_dfs.append(keep_df)

    keep_rate_df = pd.concat(keep_rate_dfs)
    columns = ['ds','server','plat','reg'] + ['d%d_keeprate' % d for d in keep_days]
    result_df = keep_rate_df[columns]
    result_df = result_df.rename(columns={'reg':'reg_user_num'})
    print result_df

    # pub、ios数据
    pub_result_df = result_df.loc[result_df.plat == 'g']
    ios_result_df = result_df.loc[result_df.plat == 'a']

    # 更新MySQL表(pub)
    table = 'dis_new_server_keep_rate'
    del_sql = 'delete from {0} where ds in {1}'.format(table, tuple(reg_dates))
    update_mysql(table, pub_result_df, del_sql,'superhero_pub')
    # 更新MySQL表(ios)
    table = 'dis_new_server_keep_rate'
    del_sql = 'delete from {0} where ds in {1}'.format(table, tuple(reg_dates))
    update_mysql(table, ios_result_df, del_sql,'superhero_ios')

if __name__ == '__main__':
    settings.set_env('superhero_bi')
    date = '20160910'
    dis_new_server(date)
