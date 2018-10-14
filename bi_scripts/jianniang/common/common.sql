---------------------------------------
-- Author      : Dong Junshuang
-- Description :
-- Time        : 2017-03-16
---------------------------------------
use jianniang_test;
-- 公会表建表
create table parse_actionlog (
    user_id string,
    server string,
    account string,
    platform string,
    appid string,
    log_t string,
    uuid string,
    coin_before int,
    coin_after int,
    coin_diff int,
    a_typ string,
    a_tar string,
    a_rst string,
    return_code string,
    session_id string
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;

-- 用户表各种经济数值
create table raw_info (
    now string,
    user_id string,
    level bigint,
    coin bigint,
    vip bigint,
    gold bigint,
    hp_bottle int,
    exp int,
    water int,
    food int,
    gas int,
    medal int,
    stone int,
    fame int,
    friendly int,
    hp_train_pill int,
    atk_train_pill int,
    def_train_pill int,
    can_use_devote int,
    teach int,
    teach_speedup int,
    last_action_time string,
    oil int,
    uuid string,
    account string,
    platform string,
    channel string,
    head_icon string,
    reg_time string,
    faction_id string,
    developed_most int,
    session_id string
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;

alter table raw_info change login_id account string;

-- 卡牌数据
create table raw_card (
    now string,
    user_id string,
    id bigint,
    card_id bigint,
    card_level int,
    atk_training int,
    def_training int,
    hp_training int,
    attr_study string,
    star int,
    working_building string,
    equip_lv string,
    chivalry_index int,
    hp int,
    atk int,
    def int,
    crit_rate int,
    crit_point int,
    dodge int,
    sword int
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;

-- 道具包裹
create table raw_item (
    now string,
    user_id string,
    id bigint,
    item_id bigint,
    amout int
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;

-- 新手引导
create table raw_guide (
    now string,
    user_id string,
    guide_done string
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;

-- 任务指引
create table raw_task_guide (
    now string,
    user_id string,
    task_id int,
    task_index int,
    task_stat int
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;

-- 战斗力排行
create table raw_sword_rank (
    server_index string,
    now string,
    user_id string,
    score float
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;

-- 建设度排行
create table raw_develop_rank (
    now string,
    user_id string,
    score float
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;

-- 测试用户
create table raw_gs (
    user_id string
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;

-- 推图
create table raw_raid (
    now string,
    user_id string,
    day_win_raids int,
    last_day_time string,
    star_award_id int,
    star_award_stat int,
    raids string,
    raids_award string
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;

-- 老用户表
create table raw_old (
    account string
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;

hdfscli upload old_20170316 /tmp
snakebite -n 192.168.1.8 ls '/tmp/'
load data inpath '/tmp/gs_20170316' into table jianniang_test.raw_gs partition (ds='20170316');

-- 充值
CREATE TABLE raw_paylog (
        order_id string,
        platform_order_id string,
        user_id string,
        level float,
        product_id string,
        old_coin float,
        gift_coin float,
        order_coin float,
        new_coin float,
        order_money float,
        order_rmb float,
        double_pay string,
        currency string,
        order_time string,
        platform string,
        channel string,
        charge_way string,
        server_index string,
        admin string,
        status string,
        update_time string
    ) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;

-- 消费
CREATE TABLE raw_spendlog (
        user_id string,
        level smallint,
        coin float,
        old_coin float,
        new_coin float,
        main_type string,
        num float,
        sub_type string,
        platform string,
        channel string,
        consume_time string,
        server_index string
    ) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;


-- mid_info_all
create table mid_info_all (
    now string,
    user_id string,
    level bigint,
    coin bigint,
    vip bigint,
    gold bigint,
    hp_bottle int,
    exp int,
    water int,
    food int,
    gas int,
    medal int,
    stone int,
    fame int,
    friendly int,
    hp_train_pill int,
    atk_train_pill int,
    def_train_pill int,
    can_use_devote int,
    teach int,
    teach_speedup int,
    last_action_time string,
    oil int,
    uuid string,
    account string,
    platform string,
    channel string,
    head_icon string,
    reg_time string,
    faction_id string,
    developed_most int,
    session_id string
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;