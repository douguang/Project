# 每日收入
'''
SELECT ds,
       sum(order_money)
FROM raw_paylog
WHERE ds >='20170216'
  AND ds <= '20170301'
  AND platform_2 <> 'admin_test'
GROUP BY ds
ORDER BY ds
'''
# 每日注册用户数
'''
SELECT ds,
       count(1)
FROM raw_reg
WHERE ds >='20170216'
  AND ds <= '20170301'
GROUP BY ds
ORDER BY ds
'''
# dau
'''
SELECT ds,
       count(distinct uid)
FROM raw_info
WHERE ds >='20170216'
  AND ds <= '20170301'
GROUP BY ds
ORDER BY ds
'''

================按周统计================
# 充值人数，充值金额
'''
SELECT platform_2,
       count(DISTINCT a.uid),
       sum(b.sum_rmb)
FROM
  (SELECT DISTINCT uid,
                   platform_2
   FROM raw_info
   WHERE ds >='20170216'
     AND ds <= '20170222')a
JOIN
  (SELECT uid,
          sum(order_rmb) sum_rmb
   FROM raw_paylog
   WHERE ds >='20170216'
     AND ds <= '20170222'
     AND platform_2 <> 'admin_test'
   GROUP BY uid)b ON a.uid = b.uid
GROUP BY a.platform_2
ORDER BY a.platform_2
'''
# DAU
'''
SELECT platform_2,
       count(DISTINCT uid)
FROM raw_info
WHERE ds >='20170216'
  AND ds <= '20170222'
GROUP BY platform_2
'''
# 注册用户
'''
SELECT platform_2,
       count(DISTINCT uid)
FROM raw_info
WHERE ds >='20170209'
  AND ds <= '20170215'
  AND regexp_replace(substr(create_time,1,10),'-','') >= '20170209'
  AND regexp_replace(substr(create_time,1,10),'-','') <= '20170215'
GROUP BY platform_2
'''
========vip用户========
# DAU
'''
SELECT vip,
       count(DISTINCT uid)
FROM
  (SELECT uid,
          max(vip_level) vip
   FROM raw_info
   WHERE ds >='20170216'
     AND ds <= '20170222'
     AND vip_level > 0
   GROUP BY uid)a
GROUP BY vip
ORDER BY vip
'''
# 新增用户
'''
SELECT vip,
       count(DISTINCT uid)
FROM
  (SELECT uid,
          max(vip_level) vip
   FROM raw_info
   WHERE ds >='20170216'
     AND ds <= '20170222'
     AND vip_level > 0
     AND regexp_replace(substr(create_time,1,10),'-','') >= '20170216'
     AND regexp_replace(substr(create_time,1,10),'-','') <= '20170222'
   GROUP BY uid)a
GROUP BY vip
ORDER BY vip
'''
# 充值人数、充值金额、arppu
'''
select vip,count(distinct a.uid),sum(sum_rmb) from
(SELECT uid,max(vip_level) vip
FROM raw_info
WHERE ds >='20170216'
  AND ds <= '20170222'
  and vip_level > 0
group by uid
)a
JOIN
  (SELECT uid,
          sum(order_rmb) sum_rmb
   FROM raw_paylog
   WHERE ds >='20170216'
     AND ds <= '20170222'
     AND platform_2 <> 'admin_test'
GROUP BY uid)b ON a.uid = b.uid
group by vip
order by vip
'''
