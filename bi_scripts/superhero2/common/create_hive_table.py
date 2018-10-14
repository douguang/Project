# 创建数据库
CREATE DATABASE IF NOT EXISTS superhero2;
# 指定默认数据库
USE superhero2;
# 建各种表
# 新增用户
CREATE TABLE IF NOT EXISTS superhero2.parse_new_user (
    user_id STRING,
    platform string,
    date string,
    time STRING
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;
# 活跃用户
CREATE TABLE IF NOT EXISTS superhero2.parse_act_user (
    user_id STRING,
    platform string,
    date string,
    time STRING
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;
# 用户信息
CREATE TABLE IF NOT EXISTS superhero2.raw_info (
    uid STRING,
    account string,
    nick string,
    platform_2 string,
    device string,
    create_time string,
    fresh_time string,
    vip_level int,
    level int,
    zuanshi int,
    gold_coin int,
    silver_coin int,
    only_access string,
    file_date STRING
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;
# 道具信息
CREATE TABLE IF NOT EXISTS superhero2.raw_item (
    uid STRING,
    fresh_time string,
    item_id int,
    num int,
    file_date string
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;
# 装备信息
CREATE TABLE IF NOT EXISTS superhero2.raw_equip (
    uid string,
    active_time string,
    getequip_time string,
    equip_id int,
    equip_level int,
    card_id int,
    basic_attribute string,
    main_attribute string,
    second_attribute string,
    legend_attribute string,
    xilian_num int,
    file_date string
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;
# 充值信息
CREATE TABLE IF NOT EXISTS superhero2_tw.raw_paylog (
    order_id string,
    level int,
    old_diamond int,
    gift_diamond int,
    order_diamond int,
    order_money decimal(10,2),
    order_rmb decimal(10,2),
    double_pay int,
    currency string,
    order_time string,
    platform string,
    product_id int,
    scheme_id string,
    raw_data string,
    user_id string,
    uin string,
    admin string,
    reason string,
    lan_sort string,
    real_product_id string
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;
# 消费信息
CREATE TABLE IF NOT EXISTS superhero2.raw_spendlog (
    spend_id string,
    user_id string,
    level int,
    subtime string,
    diamond_num int,
    diamond_1st int,
    diamond_2nd int,
    goods_type string,
    args string
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;
# 行为日志
CREATE TABLE IF NOT EXISTS superhero2.raw_action_log (
    log_date string,
    log_time string,
    environment_name string,
    server_type string,
    uid string,
    account string,
    pre_level int,
    pre_exp int,
    pre_vip_level int,
    pre_vip_exp int,
    pre_gold_coin int,
    pre_silver_coin int,
    pre_coin int,
    post_level int,
    post_exp int,
    post_vip_level int,
    post_vip_exp int,
    post_gold_coin int,
    post_silver_coin int,
    post_coin int,
    action string,
    action_status string,
    args string,
    return_value string
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;
# 卡牌信息
CREATE TABLE IF NOT EXISTS superhero2.raw_card (
    uid string,
    active_time string,
    getcard_time string,
    card_id int,
    card_level int,
    card_star_level int,
    jinjie int,
    battle_power float,
    power float,
    quick float,
    wisdom float,
    blood float,
    physical_hurt float,
    magic_hurt float,
    physical_resist float,
    magic_resist float,
    skill_1_id int,
    skill_1_level int,
    skill_2_id int,
    skill_2_level int,
    skill_3_id int,
    skill_3_level int,
    skill_4_id int,
    skill_4_level int,
    skill_5_id int,
    skill_5_level int,
    file_date string
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;

# 用户中间表
CREATE TABLE IF NOT EXISTS superhero2.mid_info_all (
    uid STRING,
    account string,
    nick string,
    platform_2 string,
    device string,
    create_time string,
    fresh_time string,
    vip_level int,
    level int,
    zuanshi int,
    gold_coin int,
    silver_coin int,
    only_access string,
    file_date STRING
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;
# 充值中间表
CREATE TABLE IF NOT EXISTS superhero2.mid_paylog_all (
    order_id string,
    level int,
    old_coin int,
    gift_coin int,
    order_coin int,
    order_money float,
    order_rmb float,
    is_double int,
    money_type string,
    order_time string,
    platform_2 string,
    conf_id int,
    platconf_id string,
    platorder_id string,
    uid string,
    plat_id string,
    admin string,
    reason string
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;

# ALTER TABLE superhero2.mid_info_all CHANGE privilege only_access string;

# rsync /Users/kaiqigu/Downloads/stg2/spendlog_20160719 hadoop@192.168.1.27:/home/data/superhero2/spendlog/spendlog_20160719

# 新注册account
CREATE TABLE IF NOT EXISTS superhero2.mid_new_account (
    account STRING,
    platform string,
    account_reg string
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;

# 卡牌表
CREATE TABLE IF NOT EXISTS superhero2.cfg_character_detail (
    card_id int,
    card_name STRING
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;

# 卡牌表
CREATE TABLE IF NOT EXISTS superhero2.raw_card_attend (
    uid STRING,
    level int,
    card_id int
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;

CREATE TABLE IF NOT EXISTS superhero2.raw_card_use_rate (
    dt STRING,
    level_ivtl STRING,
    card_id int,
    card_name STRING,
    have_user_num float,
    attend_num float,
    use_rate float
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;

# 行为日志——台服
CREATE TABLE IF NOT EXISTS superhero2_tw.parse_action_log (
   user_id string,
  server string,
  account string,
  platform string,
  diamond int,
  coin int,
  silver int,
  log_t string,
  vip int,
  level int,
  exp int,
  a_typ string,
  a_tar string,
  a_rst string,
  diamond_free_before int,
  diamond_free_after int,
  diamond_free_diff int,
  diamond_charge_before int,
  diamond_charge_after int,
  diamond_charge_diff int,
  return_code string,
  device_mac string
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;