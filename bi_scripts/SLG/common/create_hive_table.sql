-- parse_actionlog
create table parse_actionlog
(
  a_usr string,
  account string,
  platform string,
  appid string,
  lt string,
  a_typ string,
  a_tar string,
  a_rst string,
  return_code string
)  PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile



-- parse_info
create table parse_info
(
  uid string,
  account string,
  device_mask string,
  device_name string,
  name string,
  vip float,
  level float,
  exp float,
  fame_level float,
  fame float,
  reg_time string,
  metal float,
  gas float,
  anti_obj float,
  metal_speed float,
  gas_speed float,
  anti_speed float,
  alliance_id string,
  alliance_name string,
  domain_id string,
  alliance_post string,
  alliance_contribute string,
  power float,
  stamina float,
  offline_time string,
  sub_city_num float,
  fort_num float,
  sb_chapter_id string,
  sb_pass_id string,
  tiles string,
  npc_wall_tiles string,
  guide_nodes string,
  cur_main_quest string,
  time string,
  log_id float,
  platform string
)  PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile



-- parse_card
create table parse_card
(
  uid string,
  account string,
  device_mask string,
  device_name string,
  name string,
  vip float,
  level float,
  card_id string,
  card_level float,
  unit_num float,
  star float,
  awaken_lv float,
  skill string,
  home_tile string,
  cur_tile string,
  time string,
  log_id string
)  PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile


-- parse_alliance
create table parse_alliance
(
  alliance_id string,
  name string,
  power float,
  create_time string,
  member_num float,
  domain_id string,
  city_lv_num float,
  gate_lv_num float,
  technos string,
  time string,
  log_id string
)  PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile

-- parse_skill
create table parse_skill
(
  uid string,
  account string,
  device_mask string,
  device_name string,
  name string,
  vip float,
  level float,
  skill_id string,
  star float,
  time string,
  log_id string
)  PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile


-- parse_item
create table parse_item
(
  uid string,
  account string,
  device_mask string,
  device_name string,
  name string,
  vip float,
  level float,
  item_id string,
  count float,
  time string,
  log_id string
)  PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile


--parse_city
create table parse_city
(
  uid string,
  account string,
  device_mask string,
  device_name string,
  name string,
  vip float,
  level float,
  tile string,
  builds_id string,
  builds_level float,
  time string,
  log_id string
)  PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile


-- mid_info_all
create table mid_info_all
(
  uid string,
  account string,
  device_mask string,
  device_name string,
  name string,
  vip float,
  level float,
  exp float,
  fame_level float,
  fame float,
  reg_time string,
  metal float,
  gas float,
  anti_obj float,
  metal_speed float,
  gas_speed float,
  anti_speed float,
  alliance_id string,
  alliance_name string,
  domain_id string,
  alliance_post string,
  alliance_contribute string,
  power float,
  stamina float,
  offline_time string,
  sub_city_num float,
  fort_num float,
  sb_chapter_id string,
  sb_pass_id string,
  tiles string,
  npc_wall_tiles string,
  guide_nodes string,
  time string,
  log_id float
)  PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile


-- nginx日志
create table parse_nginx
( ip string,
  time string,
  gmt string,
  get_post string,
  api_type string,
  initLog string,
  eventId string,
  recordType string,
  channel string,
  deviceMac string,
  deviceName string,
  deviceVersion string,
  deviceTime string,
  deviceNet string
)  PARTITIONED BY (ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile