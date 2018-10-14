#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 上传27机器数据到hive数据表
'''

import glob
import settings
from utils import load_files_to_hive

settings.set_env('sanguo_ks')

glob_list = (
    # superhero_bi
    # ('/home/data/superhero/action_log/action_log_????????', 'raw_action_log', 'superhero_bi'),
    # ('/home/data/superhero/paylog/paylog_????????', 'raw_paylog', 'superhero_bi'),
    # ('/home/data/superhero/spendlog/spendlog_????????', 'raw_spendlog', 'superhero_bi'),
    # ('/home/data/superhero/reg_act/reg_????????', 'raw_reg', 'superhero_bi'),
    # ('/home/data/superhero/reg_act/act_????????', 'raw_act', 'superhero_bi'),
    # ('/home/data/superhero/reg_act/ios_hc_????????', 'raw_ios_hc', 'superhero_bi'),
    # ('/home/data/superhero/reg_act/mix_hc_????????', 'raw_mix_hc', 'superhero_bi'),
    # ('/home/data/superhero/log_redis/card_????????', 'raw_card', 'superhero_bi'),
    # ('/home/data/superhero/log_redis/equip_????????', 'raw_equip', 'superhero_bi'),
    # ('/home/data/superhero/log_redis/item_????????', 'raw_item', 'superhero_bi'),
    # ('/home/data/superhero/log_redis/info_????????', 'raw_info', 'superhero_bi'),
    # ('/home/data/superhero/log_redis/all_pet_????????', 'raw_pet', 'superhero_bi'),
    # ('/home/data/superhero/log_redis/all_scores_????????', 'raw_scores', 'superhero_bi'),
    # ('/home/data/superhero/log_redis/vip_info_????????', 'raw_vip_info', 'superhero_bi'),
    # ('/home/data/superhero/log_redis/card_super_step_????????', 'raw_super_step', 'superhero_bi'),
    # # superhero_vt
    # ('/home/data/superhero_vietnam/active_user/act_????????', 'raw_act', 'superhero_vt'),
    # ('/home/data/superhero_vietnam/action_log/vt_action_log_????????', 'raw_action_log', 'superhero_vt'),
    # ('/home/data/superhero_vietnam/redis_stats/active_stats_????????', 'raw_active_stats', 'superhero_vt'),
    # ('/home/data/superhero_vietnam/redis_stats/card_????????', 'raw_card', 'superhero_vt'),
    # ('/home/data/superhero_vietnam/redis_stats/equip_????????', 'raw_equip', 'superhero_vt'),
    # ('/home/data/superhero_vietnam/redis_stats/gem_????????', 'raw_gem', 'superhero_vt'),
    # ('/home/data/superhero_vietnam/redis_stats/info_????????', 'raw_info', 'superhero_vt'),
    # ('/home/data/superhero_vietnam/redis_stats/item_????????', 'raw_item', 'superhero_vt'),
    # ('/home/data/superhero_vietnam/paylog/paylog_????????', 'raw_paylog', 'superhero_vt'),
    # ('/home/data/superhero_vietnam/redis_stats/pet_????????', 'raw_pet', 'superhero_vt'),
    # ('/home/data/superhero_vietnam/new_user/reg_????????', 'raw_reg', 'superhero_vt'),
    # ('/home/data/superhero_vietnam/viso_config/ser_list_????????', 'raw_ser_list', 'superhero_vt'),
    # ('/home/data/superhero_vietnam/viso_config/father_server_map_????????', 'raw_ser_map', 'superhero_vt'),
    # ('/home/data/superhero_vietnam/redis_stats/soul_????????', 'raw_soul', 'superhero_vt'),
    # ('/home/data/superhero_vietnam/spendlog/spendlog_????????', 'raw_spendlog', 'superhero_vt'),
    # ('/home/data/superhero_vietnam/redis_stats/card_super_step_????????', 'raw_super_step', 'superhero_vt'),
    # # superhero_en
    # # superhero_tr
    # # tf-jinshan
    # ('/home/data/tf_jinshan/action_log/action_log_????????', 'raw_tf_actionlog', 'tf_jinshan'),
    # # ('/home/data/tf_jinshan/redis_stats/info_????????', 'raw_tf_info', 'tf_jinshan'),
    # ('/home/data/tf_jinshan/spendlog/spendlog_????????', 'raw_tf_spendlog', 'tf_jinshan'),
    # ('/home/data/tf_jinshan/paylog/paylog_????????', 'raw_tf_paylog', 'tf_jinshan'),
    # # tf-dena
    # ('/home/data/tf_dena/action_log/action_log_????????', 'raw_tf_actionlog', 'tf_dena'),
    # # ('/home/data/tf_dena/redis_stats/info_????????', 'raw_tf_info', 'tf_dena'),
    # ('/home/data/tf_dena/spendlog/spendlog_????????', 'raw_tf_spendlog', 'tf_dena'),
    # ('/home/data/tf_dena/paylog/paylog_????????', 'raw_tf_paylog', 'tf_dena'),
    # # tf-taiwan
    # ('/home/data/tf_taiwan/action_log/action_log_????????', 'raw_tf_actionlog', 'tf_taiwan'),
    # # ('/home/data/tf_taiwan/redis_stats/info_????????', 'raw_tf_info', 'tf_taiwan'),
    # ('/home/data/tf_taiwan/spendlog/spendlog_????????', 'raw_tf_spendlog', 'tf_taiwan'),
    # ('/home/data/tf_taiwan/paylog/paylog_????????', 'raw_tf_paylog', 'tf_taiwan'),
    # sanguo_tw
    ('/home/data/dancer_tw/spendlog/spendlog_????????', 'raw_spendlog', 'dancer_tw'),
    ('/home/data/dancer_tw/paylog/paylog_????????', 'raw_paylog', 'dancer_tw'),
    ('/home/data/dancer_tw/action_log/action_log_????????', 'raw_actionlog', 'dancer_tw'),

    # # qiku
    # ('/home/data/superhero_qiku/viso_config/ser_list_????????', 'raw_ser_list', 'superhero_qiku'),
    # ('/home/data/superhero_qiku/log_temp/qq_action_log_????????', 'raw_action_log', 'superhero_qiku'),
    # ('/home/data/superhero_qiku/active_user/act_????????', 'raw_act', 'superhero_qiku'),
    # ('/home/data/superhero_qiku/redis_stats/info_????????', 'raw_info', 'superhero_qiku'),

    # # tl
    # ('/home/data/superhero_thailand/paylog/paylog_????????', 'raw_paylog', 'superhero_tl'),
    # ('/home/data/superhero_thailand/active_user/act_????????', 'raw_act', 'superhero_tl'),
    # ('/home/data/superhero_thailand/redis_stats/info_????????', 'raw_info', 'superhero_tl'),
    # ('/home/data/superhero_thailand/spendlog/spendlog_????????', 'raw_spendlog', 'superhero_tl'),
    # ('/home/data/superhero_thailand/new_user/reg_????????', 'raw_reg', 'superhero_tl'),
    # ('/home/data/superhero_thailand/redis_stats/card_????????', 'raw_card', 'superhero_tl'),
    # ('/home/data/superhero_thailand/redis_stats/equip_????????', 'raw_equip', 'superhero_tl'),
    # ('/home/data/superhero_thailand/redis_stats/item_????????', 'raw_item', 'superhero_tl'),
    # ('/home/data/superhero_thailand/redis_stats/pet_????????', 'raw_pet', 'superhero_tl'),
    # ('/home/data/superhero_thailand/redis_stats/card_super_step_????????', 'raw_super_step', 'superhero_tl'),
    # ('/home/data/superhero_thailand/redis_stats/all_scores_????????', 'raw_scores', 'superhero_tl'),
    # ('/home/data/superhero_thailand/redis_stats/gem_????????', 'raw_gem', 'superhero_tl'),
    # ('/home/data/superhero_thailand/redis_stats/soul_????????', 'raw_soul', 'superhero_tl'),
    # ('/home/data/superhero_thailand/action_log/tl_action_log_????????', 'raw_action_log', 'superhero_tl'),

    # # superhero_usa
    # ('/home/data/superhero_usa/action_log/action_log_????????', 'raw_action_log', 'superhero_usa'),
    # ('/home/data/superhero_usa/active_user/act_????????', 'raw_act', 'superhero_usa'),
    # ('/home/data/superhero_usa/redis_stats/info_????????', 'raw_info', 'superhero_usa'),
    # ('/home/data/superhero_usa/paylog/paylog_????????', 'raw_paylog', 'superhero_usa'),
    # ('/home/data/superhero_usa/spendlog/spendlog_????????', 'raw_spendlog', 'superhero_usa'),
    # ('/home/data/superhero_usa/new_user/reg_????????', 'raw_reg', 'superhero_usa'),
    # ('/home/data/superhero_usa/redis_stats/card_????????', 'raw_card', 'superhero_usa'),
    # ('/home/data/superhero_usa/redis_stats/equip_????????', 'raw_equip', 'superhero_usa'),
    # ('/home/data/superhero_usa/redis_stats/item_????????', 'raw_item', 'superhero_usa'),
    # ('/home/data/superhero_usa/viso_config/ser_list_????????', 'raw_ser_list', 'superhero_usa'),
    # ('/home/data/superhero_usa/viso_config/father_server_map_????????', 'raw_ser_map', 'superhero_usa'),

    # # superhero_lz
    # ('/home/data/superhero_lz/paylog_data/paylog_????????', 'raw_paylog', 'superhero_lz'),
    # ('/home/data/superhero_lz/reg_data/reg_????????', 'raw_reg', 'superhero_lz'),

)

import os
import datetime
date = (datetime.date.today() - datetime.timedelta(14)).strftime('%Y%m%d')

def yield_file_table_db_par_list(glob_list):
    for file_pattern, table, db in glob_list:
        for local_path in glob.glob(file_pattern):
            yield (local_path, table, db)
            if local_path[-8:] < date:
                print '超过两周的已上传到hive的文件：', local_path
                os.remove(local_path)

load_files_to_hive(yield_file_table_db_par_list(glob_list))
