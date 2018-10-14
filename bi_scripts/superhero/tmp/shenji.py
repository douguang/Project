#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 超级英雄-审计数据(pub、ios)
'''
# 1.活跃用户数统计
# 活跃用户数
'''
select substr(ds,1,6),count(uid) from raw_info where ds >='20161101'
and ds <='20161231' group by substr(ds,1,6) order by substr(ds,1,6)
'''
# 注册用户数
'''
select substr(ds,1,6),count(distinct uid) from raw_reg
where ds >='20160101' and ds <='20161231'
group by substr(ds,1,6)
order by substr(ds,1,6)
'''
# 充值人数，充值金额
'''
select substr(ds,1,6),count(distinct uid),sum(order_money) from raw_paylog
where ds >='20161101' and ds <='20161231'
and platform_2 <> 'admin_test'
group by substr(ds,1,6) order by substr(ds,1,6)
'''
# 2.道具销售
'''
SELECT goods_type,
          sum(coin_num) sum_coin
   FROM raw_spendlog
   WHERE ds>='20161101'
    AND ds<='20161231'
    AND goods_type in
    ('gacha.do_reward_gacha',
    'shop.buy',
    'magic_school.open_contract',
    'roulette.open_roulette10',
    'one_piece.open_roulette10'
    )
   GROUP BY goods_type
'''
# 3.充值时段
'''
SELECT ivtl,
       count(DISTINCT uid) pay_user_num,
       count(order_id) pay_times,
       sum(order_money) pay_money from
  (SELECT uid,order_id,order_money,order_time, CASE
   WHEN tt >=7 AND tt <12 THEN 'a'
   WHEN tt >=12 AND tt <17 THEN 'b'
   WHEN tt >=17 AND tt <23 THEN 'c'
   WHEN tt >=23 AND tt <= 24 THEN 'd'
   WHEN tt >=0 AND tt < 7 THEN 'd' END ivtl
   FROM
     (SELECT uid,order_id,order_money,order_time,hour(order_time) tt
      FROM raw_paylog
      WHERE ds>='20161101'
        AND ds<='20161231' )a )b
GROUP BY ivtl
'''
# 审计 - 玩家充值明细
'''
select a.uid,a.sum_pay,a.pay_num,nvl(c.login_day_num,0),a.pay_day_num,b.create_time,b.last_login_time,a.first_order_time,a.last_order_time,a.level,a.sum_coin,b.day_coin_num,b.platform_2 from
(select uid,
          sum(order_money) sum_pay,
          sum(order_coin) sum_coin,
          count(order_id) pay_num,
          max(LEVEL) level,
          count(distinct to_date(order_time)) pay_day_num,
          min(order_time) first_order_time,
          max(order_time) last_order_time
      from mid_paylog_all where ds ='20161231' group by uid
)a
join (
  SELECT uid,
       platform_2,
       min(create_time) create_time,
       max(fresh_time) last_login_time,
       sum(zuanshi) day_coin_num
FROM mid_info_all
WHERE ds = '20161231'
GROUP BY uid,platform_2
)b on a.uid = b.uid
left join
(
  SELECT uid,
          count(ds) login_day_num
   FROM raw_act
   GROUP BY uid
  )c
  on b.uid = c.uid
order by a.sum_pay desc limit 3000
'''
# 审计 - 充值区间
'''
SELECT itvl,
       count(uid) user_num,
       sum(pay_times) sum_times,
       sum(sum_pay) sum_pay,
       avg(LEVEL) avg_level
FROM
  (SELECT uid,
          sum_pay,
          LEVEL,
          pay_times,
          CASE
              WHEN sum_pay < 1000 THEN '1000'
              WHEN sum_pay >= 1000
                   AND sum_pay < 3000 THEN '1000_3000'
              WHEN sum_pay >= 3000
                   AND sum_pay < 5000 THEN '3000_5000'
              WHEN sum_pay >= 5000
                   AND sum_pay < 10000 THEN '5000_10000'
              WHEN sum_pay > 10000 THEN '10001'
          END AS itvl
   FROM
     (SELECT uid,
             sum(order_money) sum_pay,
             max(LEVEL) LEVEL,
                        count(order_id) pay_times
      FROM raw_paylog
      WHERE ds>='20161101'
        AND ds<='20161231'
      GROUP BY uid)a)b
where itvl is not null
GROUP BY itvl
'''
# 查询单次充值2000以上用户
'''
select distinct uid
from mid_paylog_all where ds ='20161231' and order_money >2000
'''
# 14年-15年pub、ios、七酷的收入
'''
select substr(order_time,1,7),sum(order_money)  from total_paylog
where platform_2 <> 'admin_test'
group by substr(order_time,1,7)
order by substr(order_time,1,7)
'''
