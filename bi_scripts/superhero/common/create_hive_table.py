# 新注册account
CREATE TABLE IF NOT EXISTS superhero_tl.mid_new_account(
    account STRING,
    uid STRING,
    plat  STRING,
    platform_2 STRING,
    server STRING
) PARTITIONED BY(ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile

# 卡牌表
CREATE TABLE IF NOT EXISTS superhero_bi.cfg_character_detail(
    card_id int,
    card_name STRING
) PARTITIONED BY(ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile

# 卡牌表
CREATE TABLE IF NOT EXISTS superhero_qiku.cfg_character_detail(
    card_id int,
    card_name STRING
) PARTITIONED BY(ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile

# 卡牌表
CREATE TABLE IF NOT EXISTS superhero_tl.cfg_character_detail(
    card_id int,
    card_name STRING
) PARTITIONED BY(ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile

# 卡牌表
CREATE TABLE IF NOT EXISTS superhero_vt.cfg_character_detail(
    card_id int,
    card_name STRING
) PARTITIONED BY(ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile

# 卡牌表
CREATE TABLE IF NOT EXISTS superhero_self_en.cfg_character_detail(
    card_id int,
    card_name STRING
) PARTITIONED BY(ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile

# 法宝表
CREATE TABLE IF NOT EXISTS superhero_bi.raw_talisman(
    uid STRING comment '用户id',
    login_time STRING,
    get_time STRING,
    c_id STRING,
    type_id STRING,
    c_level float,
    c_star float
) PARTITIONED BY(ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile

# 测试用户表
CREATE TABLE IF NOT EXISTS superhero_bi.mid_gs(
    uid STRING comment '用户id'
) PARTITIONED BY(ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile

# 创建中间表
CREATE TABLE IF NOT EXISTS superhero_bi.mart_assist(
    user_id string comment '用户ID', name string comment '用户名', server string comment '服', platform string comment '用户渠道', plat string comment '平台', account string comment '账号', level float comment '等级', vip float comment 'VIP等级', reg_time string comment '注册时间', act_time string comment '活跃时间', order_money float comment '充值金额', coin float comment '身上的钻石', order_coin float comment '充值获得的钻石', spend_coin float comment '消费的钻石'
) PARTITIONED BY(ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile

# 创建中间表(周表，每天四)
CREATE TABLE IF NOT EXISTS superhero_bi.mart_w_assist(
    user_id string comment '用户ID', name string comment '用户名', server string comment '服', platform string comment '用户渠道', plat string comment '平台', account string comment '账号', level float comment '等级', vip float comment 'VIP等级', reg_time string comment '注册时间', act_time string comment '活跃时间', order_money float comment '充值金额', coin float comment '身上的钻石', order_coin float comment '充值获得的钻石', spend_coin float comment '消费的钻石'
) PARTITIONED BY(ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile

# 创建中间表（月表，每月1日跑批）
CREATE TABLE IF NOT EXISTS superhero_bi.mart_m_assist(
    user_id string comment '用户ID', name string comment '用户名', server string comment '服', platform string comment '用户渠道', plat string comment '平台', account string comment '账号', level float comment '等级', vip float comment 'VIP等级', reg_time string comment '注册时间', act_time string comment '活跃时间', order_money float comment '充值金额', coin float comment '身上的钻石', order_coin float comment '充值获得的钻石', spend_coin float comment '消费的钻石'
) PARTITIONED BY(ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile

# 创建用户中间表（月表，每月1日跑批）
CREATE TABLE IF NOT EXISTS superhero_bi.mart_m_assist(
    user_id string comment '用户ID', name string comment '用户名', server string comment '服', platform string comment '用户渠道', plat string comment '平台', account string comment '账号', level float comment '等级', vip float comment 'VIP等级', reg_time string comment '注册时间', act_time string comment '活跃时间', order_money float comment '充值金额', coin float comment '身上的钻石', order_coin float comment '充值获得的钻石', spend_coin float comment '消费的钻石'
) PARTITIONED BY(ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile

# 创建卡牌中间表
CREATE TABLE IF NOT EXISTS superhero_bi.mart_card(
    card_id float comment '卡牌ID', card_name string comment '卡牌名称', user_id string comment '用户UID', is_fight float comment '是否上阵', jinjie float comment '进阶阶数', zhuansheng float comment '转生', name string comment '用户名', server string comment '服务器', platform string comment '渠道', plat string comment '平台', account string comment '账号', level float comment '等级', vip float comment 'VIP等级'
) PARTITIONED BY(ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile

# 所有用户历史充值总额
CREATE TABLE IF NOT EXISTS superhero_tw.mart_paylog(
    user_id string comment 'user_id', order_money float comment '当日充值总额', order_coin float comment '当日充值钻石总数', history_money float comment '历史充值总额', history_coin float comment '当日充值钻石总数', is_new_pay float comment '是否是新增充值用户UID'
) PARTITIONED BY(ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile

# 创建国内英魂表
CREATE TABLE IF NOT EXISTS superhero_bi.raw_soul(
    uid string comment 'uid',
    active_name string comment '活跃时间',
    create_time string comment '英魂获得时间',
    soul_id float comment '英魂ID',
    soul_conf_id float comment '英魂配置ID',
    is_fight float comment '上阵位置',
    soul_lv float comment '等级',
    current_exp float comment '当前经验',
    next_req_exp float comment '下一级升级需要的经验'
) PARTITIONED BY(ds STRING)
ROW format delimited FIELDS TERMINATED BY '\t' stored AS textfile;



alter table raw_talisman change column uid uid  STRING comment '用户ID'
alter table raw_talisman change column c_id c_id  STRING comment '法宝ID'
alter table raw_talisman change column type_id type_id  STRING comment '法宝的种类'
alter table raw_talisman change column c_level c_level  STRING comment '法宝等级'
alter table raw_talisman change column c_star c_star  STRING comment '法宝星级'
alter table raw_soul change column soul_id soul_id  STRING comment '英魂ID';

alter table mart_assist  ADD COLUMNS(history_money float comment '历史充值总额')
alter table mart_assist  ADD COLUMNS(history_coin float comment '历史充值钻石总数')
alter table mart_assist  ADD COLUMNS(is_new_pay float comment '是否是新增充值用户')
alter table mart_assist  ADD COLUMNS(is_new_user float comment '是否是新用户')
ALTER TABLE mart_paylog ADD COLUMNS(account string comment 'account')

load data inpath '/tmp/all_talisman_20170412' into table superhero_bi.raw_talisman partition(ds='20170412')

load data inpath '/tmp/paylog_all' into table superhero_vt.total_paylog partition(ds='20170418')

load data inpath '/tmp/mart_paylog_20170401' into table superhero_tw.mart_paylog partition(ds='20170401');
load data inpath '/tmp/mart_paylog_20170331' into table superhero_vt.mart_paylog partition(ds='20170331');



