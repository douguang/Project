# 常规充值数据
'''
select ds,sm,count(distinct user_id) uid_num
from (
select
ds,user_id,sum_money,
case when sum_money = 1 then '1'
when sum_money = 1 then '1'
when sum_money >= 2 and sum_money<=5 then '2_5'
when sum_money = 6 then '6'
when sum_money >= 7 and sum_money<=29 then '7_29'
when sum_money = 30 then '30'
when sum_money >= 31 and sum_money<=49 then '31_49'
when sum_money = 50 then '50'
when sum_money >= 51 and sum_money<=99 then '51_99'
when sum_money = 100 then '100'
when sum_money >= 101 and sum_money<=299 then '101_299'
when sum_money = 300 then '300'
when sum_money >= 301 and sum_money<=499 then '301_499'
when sum_money = 500 then '500'
when sum_money >= 501 and sum_money<=799 then '501_799'
when sum_money = 800 then '800'
when sum_money >= 801 and sum_money<=999 then '801_999'
when sum_money = 1000 then '1000'
when sum_money >= 1001 and sum_money<=1499 then '1001_1499'
when sum_money = 1500 then '1500'
when sum_money >= 1501 and sum_money<=1999 then '1501_1999'
when sum_money = 2000 then '2000'
when sum_money >= 2001 and sum_money<=2999 then '2001_2999'
when sum_money = 3000 then '3000'
when sum_money >= 3001 and sum_money<=3999 then '3001_3999'
when sum_money = 4000 then '4000'
when sum_money >= 4001 then '4001'
end sm
from
(
select ds,user_id,sum(order_money) sum_money from raw_paylog where ds >= '20160907' and ds <='20160917' group by ds,user_id
)a
)b
group by ds,sm
order by ds,sm
'''
# 充值总额、人数
'''
select ds,reverse(substring(reverse(user_id), 8)) AS server,sum(order_money),count(distinct user_id)
from raw_paylog
where ds >='20160907' and ds <='20160917'
and reverse(substring(reverse(user_id), 8)) >= 'tw0' and reverse(substring(reverse(user_id), 8)) <= 'tw2'
group by ds, reverse(substring(reverse(user_id), 8))
order by ds, reverse(substring(reverse(user_id), 8))
'''

# 消费数据
'''
select ds,reverse(substring(reverse(user_id), 8)),goods_type,sum(coin_num) coin_num from raw_spendlog
where ds >='20160907' and ds <='20160917'
and reverse(substring(reverse(user_id), 8)) >= 'tw0' and reverse(substring(reverse(user_id), 8)) <= 'tw2'
group by ds,goods_type,reverse(substring(reverse(user_id), 8))
order by ds,goods_type,reverse(substring(reverse(user_id), 8))
'''
