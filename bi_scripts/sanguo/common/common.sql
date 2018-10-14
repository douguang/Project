invalidate metadata;
set APPX_COUNT_DISTINCT=true;
select ds,
       reverse(substring(reverse(user_id), 8)) as server,
       count(distinct case when product_id = '1' then user_id else null end) as month_num,
       count(distinct case when product_id = '2' then user_id else null end) as season_num
from
(
  select user_id, product_id, ds
  from sanguo_tx.raw_paylog
  where product_id in ('1', '2')
  union all
  select user_id, product_id, ds
  from sanguo_ks.raw_paylog
  where product_id in ('1', '2')
) t
group by server, ds



invalidate metadata;
use sanguo_tx;
create table tmp_r_level (user_id string, server string, r_level string);
insert overwrite table tmp_r_level
select t1.user_id as user_id,
       reverse(substring(reverse(t1.user_id), 8)) as server,
       case when pay_rmb > 2000 then 'big_r'
            when pay_rmb > 500 and pay_rmb <= 2000 then 'mid_r'
            when pay_rmb > 0 and pay_rmb <= 500 then 'small_r'
            else 'free'
            end as r_level
from
(
    select user_id
    from mid_info_all
    where ds = '20160426'
) t1
left outer join
(
    select user_id,
           sum(order_money) as pay_rmb
    from mid_paylog_without_gs
    group by user_id
) t2 on t1.user_id = t2.user_id


-- 解析动作日志
use sanguo_ks;
drop table tmp_parsed_action;
create table if not exists tmp_parsed_action (
  user_id string,
  action string,
  diff bigint,
  obj string,
  ds string
);
insert overwrite table tmp_parsed_action
select case when body.a_typ = 'Dmp102001'
            then split(body.a_usr, '@')[1]
            else split(body.a_usr, '@')[0]
            end as user_id,
       body.a_typ as action,
       cast(rst.diff as bigint) as diff,
       rst.obj as obj,
       ds
from raw_actionlog
LATERAL VIEW explode(body.a_rst) rst_tab as rst
where ds >= '20160419' and ds <= '20160427' and rst.obj in ('Dmp:FreeMoney', 'Dmp:Money')
;


-- 日期、服务器、r_level、dau、钻石存量
invalidate metadata;
use sanguo_tx;
select ds,
       server,
       r_level,
       count(t1.user_id) as dau,
       sum(coin) as remain_coin
from
tmp_r_level
join
(
  select user_id,
         coin,
         ds
  from raw_info
) t1 on t1.user_id = tmp_r_level.user_id
group by ds, server, r_level



-- 钻石消耗、免费钻石、付费钻石
invalidate metadata;
use sanguo_tx;
select ds,
       server,
       r_level,
       sum(case when diff < 0 then -diff else 0 end) as consume,
       sum(case when diff > 0 and obj = 'Dmp:FreeMoney' then diff else 0 end) as free_get,
       sum(case when diff > 0 and obj = 'Dmp:Money' then diff else 0 end) as charge_get
from
tmp_r_level
left outer join
tmp_parsed_action
on tmp_parsed_action.user_id = tmp_r_level.user_id
group by ds, server, r_level


-- 创建解析过的actionlog
use sanguo_tx;
create table mid_actionlog (
  user_id string,
  server string,
  account string,
  coin bigint,
  coin_free bigint,
  coin_charge bigint,
  log_t string,
  vip_level int,
  level int,
  combat bigint,
  food bigint,
  a_typ string,
  a_tar string,
  a_rst string,
  FreeMoney_before bigint,
  FreeMoney_after bigint,
  FreeMoney_diff bigint,
  Money_before bigint,
  Money_after bigint,
  Money_diff bigint,
  return_code string
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;


-- 公会表建表
use sanguo_tx;
create table raw_association (
    server string,
    ass_id string,
    name string,
    guild_lv int,
    player_num int,
    science string,
    owner string,
    vp string,
    sold_log string,
    player string
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;
load data inpath '/tmp/all_association_20160509' into table raw_association partition (ds='20160509');

