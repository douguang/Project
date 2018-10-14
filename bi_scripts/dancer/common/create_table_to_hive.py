CREATE TABLE IF NOT EXISTS dancer_tw.ext_activeuser (
    user_id STRING,
    account STRING,
    reg_time  STRING,
    act_time STRING,
    platform STRING
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;

# 注册用户数据
CREATE TABLE IF NOT EXISTS dancer_tw.raw_registeruser (
    user_id STRING,
    account STRING,
    reg_time  STRING
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;

# 201060912及之前的所有info数据
create table dancer_tw.raw_12ago_info(
    user_id     string
,   account     string
,   name        string
,   reg_time        float
,   act_time        int
,   level       int
,   vip     int
,   free_coin       int
,   charge_coin     int
,   gold        int
,   energy      int
,   cmdr_energy     int
,   honor       int
,   combat      int
,   guide       int
,   max_stage       int
,   item_dict       string
,   card_dict       string
,   equip_dict      string
,   combiner_dict       string
,   once_reward     string
,   card_assistant      string
,   combiner_in_use     string
,   card_assis_active       string
,   chips       string
,   chip_pos        string
,   equip_pos       string
,   device_mark     string
,   battleship      string
,   cannons     string
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;


# 韩国版参考多语言版表结构
create database dancer_kr

create table mart_assist like dancer_mul.mart_assist

create table mid_assist like dancer_mul.mid_assist

create table mid_info_all like dancer_mul.mid_info_all

create table mid_new_account like dancer_mul.mid_new_account

create table parse_actionlog like dancer_mul.parse_actionlog

create table parse_info like dancer_mul.parse_info

create table parse_nginx like dancer_mul.parse_nginx

create table parse_sdk_nginx like dancer_mul.parse_sdk_nginx

create table parse_voided_data like dancer_mul.parse_voided_data

create table raw_association like dancer_mul.raw_association

create table raw_paylog like dancer_mul.raw_paylog

create table raw_spendlog like dancer_mul.raw_spendlog