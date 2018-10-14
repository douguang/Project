create databases dancer_tx_beta;
CREATE TABLE IF NOT EXISTS dancer_tx_beta.ext_activeuser (
    user_id STRING,
    account STRING,
    reg_time  STRING,
    act_time STRING,
    platform STRING
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;

CREATE TABLE IF NOT EXISTS dancer_tx_beta.raw_registeruser (
    user_id STRING,
    account STRING,
    reg_time  STRING
) PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;

create table dancer_tx_beta.raw_paylog(
    order_id    string
,   admin   string
,   gift_coin   int
,   level   int
,   old_coin    int
,   order_coin  int
,   order_money int
,   order_rmb   int
,   currency_type   string
,   order_time  string
,   platform_2  string
,   product_id  string
,   raw_data    string
,   reason  string
,   scheme_id   string
,   user_id string
)   PARTITIONED BY  (ds STRING)
ROW format  delimited   FIELDS  TERMINATED  BY  '\t'    stored  AS  textfile;

create table dancer_tx_beta.raw_spendlog(
    order_id    string
,   user_id string
,   level   smallint
,   subtime string
,   coin_num    int
,   coin_1st    int
,   coin_2nd    int
,   goods_type  string
,   goods_subtype   string
,   goods_name  string
,   goods_num   int
,   goods_price string
,   goods_cnname    string
,   args    string
)   PARTITIONED BY  (ds STRING)
ROW format  delimited   FIELDS  TERMINATED  BY  '\t'    stored  AS  textfile;


create table dancer_tx_beta.mid_actionlog(
    user_id string
,   server  string
,   account string
,   log_t   string
,   vip int
,   level   int
,   coin_charge bigint
,   coin_free   bigint
,   platform    string
,   a_typ   string
,   a_tar   string
,   freemoney_before    bigint
,   freemoney_after bigint
,   freemoney_diff  bigint
,   money_before    bigint
,   money_after bigint
,   money_diff  bigint
,   return_code string
)   PARTITIONED BY  (ds STRING)
ROW format  delimited   FIELDS  TERMINATED  BY  '\t'    stored  AS  textfile;

create table dancer_tx_beta.parse_info(
    user_id string
,   account string
,   name    string
,   reg_time    string
,   act_time    string
,   level   int
,   vip int
,   free_coin   int
,   charge_coin int
,   gold    int
,   energy  int
,   cmdr_energy int
,   honor   int
,   combat  int
,   guide   int
,   max_stage   int
,   item_dict   string
,   card_dict   string
,   equip_dict  string
,   combiner_dict   string
,   once_reward string
,   card_assistant  string
,   combiner_in_use string
,   card_assis_active   string
,   chips   string
,   chip_pos    string
,   equip_pos   string
,   device_mark string
)   PARTITIONED BY  (ds STRING)
ROW format  delimited   FIELDS  TERMINATED  BY  '\t'    stored  AS  textfile;

create table dancer_tx_beta.raw_association(
    server  string
,   ass_id  string
,   name    string
,   guild_lv    int
,   player_num  int
,   ass_tech    string
,   owner   string
,   vp  string
,   vc  string
,   sold_log    string
,   player  string
,   goods   string
,   dedication_log  string
,   ass_talent_data string
)   PARTITIONED BY  (ds STRING)
ROW format  delimited   FIELDS  TERMINATED  BY  '\t'    stored  AS  textfile;

create table dancer_tx_beta.raw_actionlog(
    body    struct<a_rst:array<struct<after:string,before:string,diff:string,obj:string>>,a_tar:string,a_typ:string,a_usr:string>
,   log_t   bigint
,   app_id  string
,   app_ver string
,   device_id   int
,   param5  string
,   mobage_uid  string
,   param4  int
,   param3  int
,   param2  int
,   param1  int
,   device_mark string
)   PARTITIONED BY  (ds STRING)
ROW format  delimited   FIELDS  TERMINATED  BY  '\t'    stored  AS  textfile;
