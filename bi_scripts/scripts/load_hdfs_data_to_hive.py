#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 把hdfs上/data路径下的文件载入hive表
'''
import sqlalchemy
from hdfs import InsecureClient
import os
import settings
from settings import hive_template

settings.set_env('sanguo_ks')

hdfs_client = InsecureClient('http://192.168.1.8:50070')
engine = sqlalchemy.create_engine(hive_template.format(db=''))
conn_hive = engine.raw_connection()

path_table_dic = {
    # tf_taiwan
    '/data/tf_taiwan/spendlog/spendlog_': ('tf_taiwan', 'raw_tf_spendlog'),
    '/data/tf_taiwan/paylog/paylog_': ('tf_taiwan', 'raw_tf_paylog'),
    '/data/tf_taiwan/info/info_': ('tf_taiwan', 'raw_tf_info'),
    '/data/tf_taiwan/action_log/action_log_': ('tf_taiwan', 'raw_tf_actionlog'),
    # tf_jinshan
    '/data/tf_jinshan/spendlog/spendlog_': ('tf_jinshan', 'raw_tf_spendlog'),
    '/data/tf_jinshan/paylog/paylog_': ('tf_jinshan', 'raw_tf_paylog'),
    '/data/tf_jinshan/info/info_': ('tf_jinshan', 'raw_tf_info'),
    '/data/tf_jinshan/action_log/action_log_': ('tf_jinshan', 'raw_tf_actionlog'),
    # tf_dena
    '/data/tf_dena/spendlog/spendlog_': ('tf_dena', 'raw_tf_spendlog'),
    '/data/tf_dena/paylog/paylog_': ('tf_dena', 'raw_tf_paylog'),
    '/data/tf_dena/info/info_': ('tf_dena', 'raw_tf_info'),
    '/data/tf_dena/action_log/action_log_': ('tf_dena', 'raw_tf_actionlog'),
    # superhero_bi
    '/data/superhero/act/act_': ('superhero_bi', 'raw_act'),
    '/data/superhero/action_log/action_log_': ('superhero_bi', 'raw_action_log'),
    '/data/superhero/active_stats/active_stats_': ('superhero_bi', 'raw_active_stats'),
    '/data/superhero/card/card_': ('superhero_bi', 'raw_card'),
    '/data/superhero/equip/equip_': ('superhero_bi', 'raw_equip'),
    '/data/superhero/info/info_': ('superhero_bi', 'raw_info'),
    '/data/superhero/ios_hc/ios_hc_': ('superhero_bi', 'raw_ios_hc'),
    '/data/superhero/item/item_': ('superhero_bi', 'raw_item'),
    '/data/superhero/mix_hc/mix_hc_': ('superhero_bi', 'raw_mix_hc'),
    '/data/superhero/paylog/paylog_': ('superhero_bi', 'raw_paylog'),
    '/data/superhero/pet/all_pet_': ('superhero_bi', 'raw_pet'),
    '/data/superhero/reg/reg_': ('superhero_bi', 'raw_reg'),
    '/data/superhero/scores/all_scores_': ('superhero_bi', 'raw_scores'),
    '/data/superhero/vip_info/vip_info_': ('superhero_bi', 'raw_vip_info'),
    '/data/superhero/card/card_super_step/card_super_step_': ('superhero_bi', 'raw_super_step'),
    # superhero_en
    '/data/superhero_en/act/act_': ('superhero_en', 'raw_act'),
    '/data/superhero_en/action_log/action_log_': ('superhero_en', 'raw_action_log'),
    '/data/superhero_en/card/card_': ('superhero_en', 'raw_card'),
    '/data/superhero_en/equip/equip_': ('superhero_en', 'raw_equip'),
    '/data/superhero_en/info/info_': ('superhero_en', 'raw_info'),
    '/data/superhero_en/item/item_': ('superhero_en', 'raw_item'),
    '/data/superhero_en/paylog/paylog_': ('superhero_en', 'raw_paylog'),
    '/data/superhero_en/reg/reg_': ('superhero_en', 'raw_reg'),
    '/data/superhero_en/spendlog/spendlog_': ('superhero_en', 'raw_spendlog'),
    '/data/superhero_en/super_step/card_super_step_': ('superhero_en', 'raw_super_step'),
    # superhero_qiku
    '/data/superhero_qk/scores/all_scores_': ('superhero_qiku', 'raw_scores'),
    '/data/superhero_qk/pet/pet_': ('superhero_qiku', 'raw_pet'),
    '/data/superhero_qk/act/act_': ('superhero_qiku', 'raw_act'),
    '/data/superhero_qk/action_log/qq_action_log_': ('superhero_qiku', 'raw_action_log'),
    '/data/superhero_qk/card/card_': ('superhero_qiku', 'raw_card'),
    '/data/superhero_qk/equip/equip_': ('superhero_qiku', 'raw_equip'),
    '/data/superhero_qk/info/info_': ('superhero_qiku', 'raw_info'),
    '/data/superhero_qk/item/item_': ('superhero_qiku', 'raw_item'),
    '/data/superhero_qk/paylog/paylog_': ('superhero_qiku', 'raw_paylog'),
    '/data/superhero_qk/reg/reg_': ('superhero_qiku', 'raw_reg'),
    '/data/superhero_qk/spendlog/spendlog_': ('superhero_qiku', 'raw_spendlog'),
    '/data/superhero_qk/super_step/card_super_step_': ('superhero_qiku', 'raw_super_step'),
    # superhero_tl
    '/data/superhero_tl/act/act_': ('superhero_tl', 'raw_act'),
    '/data/superhero_tl/action_log/tl_action_log_': ('superhero_tl', 'raw_action_log'),
    '/data/superhero_tl/card/card_': ('superhero_tl', 'raw_card'),
    '/data/superhero_tl/equip/equip_': ('superhero_tl', 'raw_equip'),
    '/data/superhero_tl/gem/gem_': ('superhero_tl', 'raw_gem'),
    '/data/superhero_tl/info/info_': ('superhero_tl', 'raw_info'),
    '/data/superhero_tl/item/item_': ('superhero_tl', 'raw_item'),
    '/data/superhero_tl/paylog/paylog_': ('superhero_tl', 'raw_paylog'),
    '/data/superhero_tl/pet/pet_': ('superhero_tl', 'raw_pet'),
    '/data/superhero_tl/reg/reg_': ('superhero_tl', 'raw_reg'),
    '/data/superhero_tl/scores/all_scores_': ('superhero_tl', 'raw_scores'),
    '/data/superhero_tl/soul/soul_': ('superhero_tl', 'raw_soul'),
    '/data/superhero_tl/spendlog/spendlog_': ('superhero_tl', 'raw_spendlog'),
    '/data/superhero_tl/super_step/card_super_step_': ('superhero_tl', 'raw_super_step'),
    # superhero_usa
    '/data/superhero_usa/act/act_': ('superhero_usa', 'raw_act'),
    '/data/superhero_usa/action_log/action_log_': ('superhero_usa', 'raw_action_log'),
    '/data/superhero_usa/card/card_': ('superhero_usa', 'raw_card'),
    '/data/superhero_usa/equip/equip_': ('superhero_usa', 'raw_equip'),
    # '/data/superhero_usa/gem/gem_': ('superhero_usa', 'raw_gem'),
    '/data/superhero_usa/info/info_': ('superhero_usa', 'raw_info'),
    '/data/superhero_usa/item/item_': ('superhero_usa', 'raw_item'),
    '/data/superhero_usa/paylog/paylog_': ('superhero_usa', 'raw_paylog'),
    # '/data/superhero_usa/pet/all_pet_': ('superhero_usa', 'raw_pet'),
    '/data/superhero_usa/reg/reg_': ('superhero_usa', 'raw_reg'),
    # '/data/superhero_usa/scores/all_scores_': ('superhero_usa', 'raw_scores'),
    # '/data/superhero_usa/soul/soul_': ('superhero_usa', 'raw_soul'),
    '/data/superhero_usa/spendlog/spendlog_': ('superhero_usa', 'raw_spendlog'),
    # '/data/superhero_usa/super_step/card_super_step_': ('superhero_usa', 'raw_super_step'),
    # superhero_vt
    '/data/superhero_vt/act/act_': ('superhero_vt', 'raw_act'),
    '/data/superhero_vt/action_log/vt_action_log_': ('superhero_vt', 'raw_action_log'),
    '/data/superhero_vt/active_stats/active_stats_': ('superhero_vt', 'raw_active_stats'),
    '/data/superhero_vt/card/card_': ('superhero_vt', 'raw_card'),
    '/data/superhero_vt/equip/equip_': ('superhero_vt', 'raw_equip'),
    '/data/superhero_vt/gem/gem_': ('superhero_vt', 'raw_gem'),
    '/data/superhero_vt/info/info_': ('superhero_vt', 'raw_info'),
    '/data/superhero_vt/item/item_': ('superhero_vt', 'raw_item'),
    '/data/superhero_vt/paylog/paylog_': ('superhero_vt', 'raw_paylog'),
    '/data/superhero_vt/pet/pet_': ('superhero_vt', 'raw_pet'),
    '/data/superhero_vt/reg/reg_': ('superhero_vt', 'raw_reg'),
    '/data/superhero_vt/scores/all_scores_': ('superhero_vt', 'raw_scores'),
    '/data/superhero_vt/soul/soul_': ('superhero_vt', 'raw_soul'),
    '/data/superhero_vt/spendlog/spendlog_': ('superhero_vt', 'raw_spendlog'),
    '/data/superhero_vt/super_step/card_super_step_': ('superhero_vt', 'raw_super_step'),
}
try:
    cur = conn_hive.cursor()
    for dpath, _, fnames in hdfs_client.walk('/data'):
        for fname in fnames:
            hdfs_path = os.path.join(dpath, fname)
            print hdfs_path
            path_tag = hdfs_path[:-8]
            if path_tag in path_table_dic:
                db, table = path_table_dic[path_tag]
            else:
                continue
            partition = hdfs_path[-8:]
            # 检查文件是否已经存在
            filename = os.path.basename(hdfs_path)
            hive_path = '/user/hive/warehouse/{db}.db/{table}/ds={partition}/{filename}'.format(
                db=db,
                table=table,
                filename=filename,
                partition=partition)
            if hdfs_client.status(hive_path, strict=False):
                print '{hdfs_path} already in hive!'.format(
                    hdfs_path=hdfs_path)
                print hive_path
                hdfs_client.delete(hdfs_path, recursive=True)
                continue
            # 载入文件到表
            hql = '''
            load data inpath '{remote_path}'
            overwrite into table {db}.{table}
            partition (ds='{partition}')
            '''.format(**{
                'partition': partition,
                'remote_path': hdfs_path,
                'table': table,
                'db': db,
            })
            print hql
            cur.execute(hql)
            print 'Success: {hdfs_path} -> {db}.{table} partition: {partition}\n'.format(
                **{
                    'table': table,
                    'db': db,
                    'hdfs_path': hdfs_path,
                    'partition': partition
                })
finally:
    conn_hive.close()
