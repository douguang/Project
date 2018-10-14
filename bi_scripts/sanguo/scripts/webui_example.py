#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : BI web端接入示例
'''
import sqlalchemy
import pandas as pd
from jinja2 import Template
import numpy as np


def sql_to_df(hql, db):
    '''将sql检索出结果放入dataframe中返回
    '''
    url = 'mysql+pymysql://readonly:readonly@192.168.1.27/{db}'.format(db=db)
    # print url
    engine = sqlalchemy.create_engine(url)
    conn = engine.raw_connection()
    try:
        df = pd.read_sql(hql, conn)
    finally:
        conn.close()
    return df

# 新增用户付费比例
# df = sql_to_df('select * from dis_common_newpay_rate where ds >= "20160513" and ds <= "20160517"', 'sanguo_ks')
# pay_rate_cols = [u'd1_pay_rate', u'd2_pay_rate', u'd3_pay_rate', u'd7_pay_rate', u'd15_pay_rate', u'd30_pay_rate']
# all_reg_num = df['reg_num'].sum()
# cols_to_show = [all_reg_num] + [(df[col] * df['reg_num']).sum() / all_reg_num for col in pay_rate_cols]
# # 形如 [45718, 0.062601163655452985, 0.01360514458200272, 0.0040027997725184819, 0.0052495734721553907, 0.0026466599588783422, 0.00080930924362395513]，对应表中各列

# 付费用户生命周期
# df = sql_to_df('select * from dis_common_pay_loss_raw where reg_time >= "2016-06-01" and reg_time <= "2016-06-15"', 'sanguo_ks')
# cols_to_show = [df.account.count(), df.is_pay.sum(), df.is_lost.sum(), df.life_day.median(), df.life_day.mean()]
# [47642, 4012, 12054, 0.0, 0.97586163469207843]

# 新增uid等级分布
# df = sql_to_df('select * from dis_common_uid_level_raw where ds >= "20160501" and ds <= "20160515"', 'sanguo_ks')
# print df
# cols_to_show = [df.user_id.count(), df.d8_level.mean(), df.d8_level.median(), df.d15_level.mean(), df.d15_level.median(), df.d31_level.mean(), df.d31_level.median()]
# print cols_to_show

# 新增uid等级分布详情
# level_df = sql_to_df(
#     'select * from dis_common_uid_level_raw where ds >= "20160501" and ds <= "20160515"',
#     'sanguo_ks')
# level_list = range(0, 181, 5)
# level_days = [8, 15, 31]
# show_df = pd.DataFrame((['({0}, {1}]'.format(level_list[i], level_list[i + 1])] + [level_df.query('d{2}_level>{0} and d{2}_level<={1}'.format(level_list[i], level_list[i + 1], lv))['d{0}_level'.format(lv)].count() for lv in level_days] for i in xrange(len(level_list) - 1)), columns=['lv_range'] + ['lv{0}_num'.format(day) for day in level_days])
#       lv_range  lv8_num  lv15_num  lv31_num
# 0       (0, 5]    24571     24517     24479
# 1      (5, 10]     9899      9834      9825

# # 加权平均留存
# df = sql_to_df(
#     'select * from dis_keep_rate where ds >= "20160501" and ds <= "20160515"',
#     'sanguo_ks')
# print df.head(1)
# keep_rate_days = [2, 3, 4, 5, 6, 7, 14, 30, 60, 90]
# columns = ['d{0}_keeprate'.format(d) for d in keep_rate_days]
# all_reg_num = df.reg.sum()
# columns_to_show = [all_reg_num] + [(
#     df.reg * df['d{0}_keeprate'.format(d)]).sum() / all_reg_num
#                                    for d in keep_rate_days]
# # [144046.0, 0.29232328561709459, 0.18718326090276705, 0.15360370992599584, 0.13204809574719178, 0.1191633228274302, 0.1105966149702179, 0.05063660219652056, 0.017230606889465878, 0.0, 0.0]

# 加权平均LTV
# df = sql_to_df(
#     'select * from dis_ltv where ds >= "20160522" and ds <= "20160623"',
#     'sanguo_ks')
# ltv_days = [3, 7, 14, 15, 30, 60]
# all_reg_num = df.reg_num.sum()
# # print df
# row_to_show = [all_reg_num]
# for ltv_day in ltv_days:
#     # 过滤ltv是0的，不算入加权平均
#     df_effective = df[df['d%d_ltv' % ltv_day] != 0]
#     print df_effective
#     filtered_reg_num = df_effective['reg_num'].sum()
#     if filtered_reg_num:
#         weighted_ltv = (df_effective['d%d_ltv' % ltv_day] *
#                         df_effective['reg_num']).sum() / filtered_reg_num
#         mean_pay_num = df_effective['d%d_pay_num' % ltv_day].sum()
#     else:
#         weighted_ltv = 0
#         mean_pay_num = 0
#     row_to_show.extend([mean_pay_num, weighted_ltv])
# print row_to_show
# [310003, 1188.8888888888889, 8.7220252707231865, 1293.9259259259259, 13.288029470682545, 1546.3636363636363, 17.781900654113549, 1582.2380952380952, 18.727396479093468, 2451.4000000000001, 23, 0, 0]

# 日常数据汇总
# import numpy as np
# df1 = sql_to_df('select * from dis_act_daily_info where ds >= "20160601" and ds <= "20160622"', 'sanguo_ks')
# wm_dau = lambda x: np.average(x, weights=df1.loc[x.index, "dau"])
# wm_arppu = lambda x: np.average(x, weights=df1.loc[x.index, "order_num"])
# result_df1 = df1.groupby('ds').agg({
#     'dau': 'sum',
#     'wau': 'sum',
#     'mau': 'sum',
#     'order_num': 'sum',
#     'new_pay_num': 'sum',
#     'income': 'sum',
#     'pay_rate': wm_dau,
#     'arpu': wm_dau,
#     'arppu': wm_arppu,
# }).reset_index()
# df2 = sql_to_df('select * from dis_keep_rate where ds >= "20160601" and ds <= "20160622"', 'sanguo_ks')
# wm_keeprate = lambda x: np.average(x, weights=df2.loc[x.index, "reg"])
# result_df2 = df2.groupby('ds').agg({
#     'reg': 'sum',
#     'd2_keeprate': wm_keeprate,
#     'd3_keeprate': wm_keeprate,
#     'd7_keeprate': wm_keeprate,
# }).reset_index()
# final_result = result_df1.merge(result_df2, how='left').fillna(0)
# print final_result
#          ds      dau  pay_rate     mau      arpu  new_pay_num  order_num  \
# 0  20160601  17591.0  5.781365  209917  5.683475        408.0     1017.0

#      wau   income      arppu  d7_keeprate     reg  d2_keeprate
# 0  49885  99978.0  98.306785     0.082144  4553.0     0.503624


# 分接口钻石消耗（分server、vip表）汇总
# df = sql_to_df('select * from dis_spend_api_detail where ds = "20160508" and server in ("m1", "m2") and vip in (0, 1)', 'sanguo_ks')
# result_df = df.groupby('goods_type').agg({
#     'spend_all': 'sum',
#     'user_num': 'sum',
#     'spend_num': 'sum',
#     'user_num_every': lambda x: np.average(x, weights=df.loc[x.index, "user_num"]),
#     'spend_num_every': lambda x: np.average(x, weights=df.loc[x.index, "spend_num"]),
# }).sort_values('spend_all', ascending=False)
# print result_df
#                                spend_num  spend_num_every  user_num  \
# goods_type
# card_gacha.do_gacha                 2370       348.791561       888
# card_gacha.do_limit_gacha            800       611.832500       357


# if __name__ == '__main__':
#     # 从sql取出df
#     df = sql_to_df('select * from dis_act_3day limit 10', 'sanguo_ks')
#     # 渲染df成table
#     raw_template = u'''
#     <table>
#         <tr>
#             <th>日期</th>
#             <th>vip</th>
#             <th>服务器</th>
#             <th>三日活跃</th>
#         </tr>
#         {% for _, row in df.iterrows() %}
#         <tr>
#             <th>{{ row['ds'] }}</th>
#             <th>{{ row['vip'] }}</th>
#             <th>{{ row['server'] }}</th>
#             <th>{{ row['act_3day'] }}</th>
#         </tr>
#         {% endfor %}
#     </table>
#     '''
#     t = Template(raw_template)
#     print t.render(df=df)
