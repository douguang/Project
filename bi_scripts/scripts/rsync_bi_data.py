#!/usr/bin/env python
# -*- coding:utf8 -*-

import os
import sys
import datetime

try:
    platform = sys.argv[1]
except:
    print "请输入项目名"
    sys.exit()

try:
    DATE = sys.argv[2]
except:
    DATE = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
port = "22"
if platform in ["sanguo_ks", "sanguo_ios", "sanguo_tt"]:
    username = "kaiqigu"
    IP = "120.92.2.239"
elif platform in ["sanguo_kr", "sanguo_tw", "sanguo_tl", "sanguo_mth", "dancer_tw", "superhero_tw", "dancer_mul", "jianniang_tw", "superhero2_tw", "superhero_self_en", "jianniang_bt", "crime_empire_pub"] :
    username = "kaiqigu"
    IP = "101.251.250.18"
elif platform in ["dancer_pub", "jianniang_pub", "sanguo_bt", "dancer_bt", "dancer_cgame", "metal_beta", "qiling_ks", "metal_cgame", 'dancer_cgwx', 'sanguo_xq']:
    username = "admin"
    IP = "120.92.21.179"
elif platform == "superhero2":
    username = "admin"
    IP = "120.92.213.236"
    port = "2009"
elif platform in ["dancer_tx", "sanguo_tx"]:
    username = "admin"
    IP = "123.206.51.215"
elif platform == "superhero":
    username = "admin"
    IP = "120.132.57.19"
elif platform == "superhero_qiku":
    username = "admin"
    IP = "212.64.107.139"
elif platform == "superhero_vietnam":
    username = "kaiqigu"
    IP = "101.251.250.18"
elif platform == "superhero_mul":
    username = "kaiqigu"
    IP = "101.251.250.18"
elif platform == "sanguo_guandu":
    username = "admin"
    IP = "120.92.119.58"
elif platform == "sanguo_chaov":
    username = "admin"
    IP = "154.8.216.90"
elif platform == "dancer_kr":
    username = "kaiqigu"
    IP = "211.117.60.118"
else:
    print "项目输入错误"


DST_ROOT_DIR = '/home/data/'
PLATFORM = {
    'sanguo_ks': 'bi_data/pub_js',
    'sanguo_tt': 'bi_data/pub_tt',
    'sanguo_kr': 'bi_data/pub_kr',
    'sanguo_tw': 'bi_data/pub_tw_gpn',
    'sanguo_tx': '/data/bi_data/sanguo_pub_tencent',
    'sanguo_tl': 'bi_data/pub_th',
    'sanguo_mth': 'data2_bi_data/pub_minbag_th',
    'sanguo_ios': 'bi_data/pub_ios',
    'sanguo_bt': '/data/bi_data/pub_bt',
    'sanguo_xq': '/data/bi_data/sanguo_xq',
    'sanguo_guandu': '/data/bi_data/guandu',
    'sanguo_chaov': '/data/bi_data/chaov',
    'dancer_pub': '/data/bi_data/pub_jinshan',
    'dancer_bt': '/data/bi_data/dancer_bt',
    'jianniang_pub': '/data/bi_data/h5_ks',
    'dancer_tx': '/data/bi_data/pub_tencent',
    'dancer_tw': 'bi_data/pub_taiwan_gpn',
    'dancer_mul': 'data2_bi_data/pub_multi_lan_gpn',
    'dancer_kr': 'data/bi_data/dancer_kr',
    'jianniang_tw': 'data2_bi_data/h5',
    'jianniang_bt': 'data2_bi_data/h5_bt',
    'superhero': '/data/admin/superhero/data',
    'superhero2': '/data/bi_data/superhero2',
    'superhero_vietnam': 'data2_bi_data/superhero_vt',
    'superhero_mul': 'data2_bi_data/superhero_mul',
    'superhero_qiku': '/data/admin/superhero/data',
    'superhero_self_en': 'data2_bi_data/superhero_self_en',
    'superhero_tw': 'bi_data/superhero_tw',
    'superhero2_tw': 'data2_bi_data/superhero2_tw',
    'dancer_cgame': '/data/bi_data/dancer_cgame',
    'dancer_cgwx': '/data/bi_data/dancer_cgwx',
    'metal_beta': '/data/bi_data/hjzj_ks',
    'metal_cgame': '/data/bi_data/metal_cgame',
    "crime_empire_pub": 'data2_bi_data/uw_tw'
}

if platform in ['sanguo_ks', 'sanguo_kr', 'sanguo_xq', 'sanguo_tx', 'sanguo_tw', 'sanguo_tl', 'sanguo_mth', 'sanguo_ios', 'sanguo_tt', 'sanguo_bt', 'metal_beta','metal_cgame', 'qiling_ks', 'sanguo_guandu', 'sanguo_chaov']:
    remote_local = {
        'action_log': 'action_log',
        'paylog': 'paylog',
        'spendlog': 'spendlog',
        'redis_info/city_num': 'city_num',
        'redis_info/new_user': 'new_user',
        'redis_info/active_user': 'active_user',
        'redis_info/honor_rank': 'honor_rank',
        'redis_info/all_association_info': 'redis_stats',
        'redis_info/redis_stats': 'redis_stats'
    }
elif platform in ['dancer_pub', 'dancer_tx', 'dancer_tw',]:
    remote_local = {
        'action_log': 'action_log',
        'spendlog': 'spendlog',
        'paylog': 'paylog',
        'all_association_info': 'all_association_info'
    }
elif platform in ['dancer_mul', 'dancer_kr', 'dancer_bt', 'dancer_cgame', 'dancer_cgwx']:
    remote_local = {
        'action_log': 'action_log',
        'spendlog': 'spendlog',
        'paylog': 'paylog',
        'redis_stats':'redis_stats',
        'all_association_info': 'all_association_info'
    }
elif platform in ['superhero_vietnam', 'superhero_tw', 'superhero_qiku', 'superhero']:
    remote_local = {
        'active_user': 'active_user',
        'redis_stats': 'redis_stats',
        'new_user': 'new_user',
        'paylog': 'paylog',
        'spendlog': 'spendlog'
    }
elif platform in ['superhero_self_en', 'superhero_mul']:
    remote_local = {
        'active_user': 'active_user',
        'redis_stats': 'redis_stats',
        'new_user': 'new_user',
        'paylog': 'paylog',
        'spendlog': 'spendlog',
        'action_log': 'action_log',
    }
elif platform in ['superhero2', 'superhero2_tw' ]:
    remote_local = {
        'paylog': 'paylog',
        'spendlog': 'spendlog',
        'action_log': 'action_log',
        'redis_static': 'redis_stats',
    }
elif platform in ['jianniang_tw', 'jianniang_pub', 'jianniang_bt', "crime_empire_pub" ]:
    remote_local = {
        'paylog': 'paylog',
        'spendlog': 'spendlog',
        'action_log': 'action_log',
        'redis_stats': 'redis_stats',
    }
else:
    sys.exit()


def rsync(platform, username, IP, date, port):
    if username == "admin":
        for i, j in remote_local.iteritems():
            src_dir = PLATFORM[platform] + "/" + i
            dst_dir = DST_ROOT_DIR + platform + '/' + j
            tmp_dir = DST_ROOT_DIR + platform + '/tmp'
            cmd = "rsync -avzP -e 'ssh -p{port}'  --partial-dir={tmp_dir} {username}@{IP}:{src}/*_{date} {dst}/".format(
                username=username, IP=IP, src=src_dir, dst=dst_dir, date=date, tmp_dir=tmp_dir, port=port)
            os.system(cmd)
        if platform in ['dancer_pub']:
            cmd = "rsync -avzP --partial-dir={tmp_dir} {username}@{IP}:/data/sites/dancer_backend/logs/bi_snapshot/*_{date} /home/data/dancer_pub/redis_stats/".format(
                username=username, IP=IP, date=date, tmp_dir=tmp_dir)
            os.system(cmd)
        elif platform in ['superhero2']:
            cmd = "mv /home/data/superhero2/redis_stats/act* /home/data/superhero2/active_user/ && mv /home/data/superhero2/redis_stats/reg* /home/data/superhero2/new_user/ "
            os.system(cmd)
        elif platform in ['dancer_tx']:
            cmd = "rsync -avzP --partial-dir={tmp_dir} {username}@{IP}:/data/sites/dancer_backend/logs/bi_snapshot/*_{date} /home/data/dancer_tx/redis_stats/".format(
                username=username, IP=IP, date=date, tmp_dir=tmp_dir)
            os.system(cmd)
        elif platform in ['superhero']:
            cmd = "rsync -avzP --partial-dir={tmp_dir} {username}@{IP}:/data/py_backup/tmp/*_{date} /home/data/superhero/action_log/".format(
                username=username, IP=IP, date=date, tmp_dir=tmp_dir)
            os.system(cmd)
        elif platform in ['superhero_qiku']:
            cmd1 = "rsync -avzP --partial-dir={tmp_dir} {username}@{IP}:/data/admin/superhero/data/action/*_{date}.zip /home/data/superhero_qiku/log_temp/".format(
                username=username, IP=IP, date=date, tmp_dir=tmp_dir)
            cmd2 = "cd /home/data/superhero_qiku/log_temp && unzip -o qq_action_log_{date}.zip && rm -f qq_action_log_{date}.zip".format(date=date)
            os.system(cmd1)
            os.system(cmd2)
        else:
            sys.exit()
    else:
        for i, j in remote_local.iteritems():
            src_dir = PLATFORM[platform] + "/" + i
            dst_dir = DST_ROOT_DIR + platform + '/' + j + '/'
            tmp_dir = DST_ROOT_DIR + platform + '/tmp'
            cmd = "rsync -avzP --partial-dir={tmp_dir} --password-file=/etc/rsyncd_password {username}@{IP}::{src}/*{date}* {dst}".format(
                username=username, IP=IP, src=src_dir, dst=dst_dir, date=date, tmp_dir=tmp_dir)
            os.system(cmd)
        if platform in ['superhero_vietnam']:
            cmd1 = "rsync -avzP --partial-dir={tmp_dir} --password-file=/etc/rsyncd_password {username}@{IP}::data2_bi_data/superhero_vt/action_log/*_{date}.tar.gz /home/data/superhero_vietnam/action_log/".format(
                username=username, IP=IP, date=date, tmp_dir=tmp_dir)
            cmd2 = "cd /home/data/superhero_vietnam/action_log && tar -zxvf vt_action_log_{date}.tar.gz && rm -f vt_action_log_{date}.tar.gz".format(
                date=date)
            os.system(cmd1)
            os.system(cmd2)
        elif platform in ['superhero_self_en']:
            cmd1 = "mv /home/data/superhero_self_en/spendlog/spendlog_{date} /home/data/superhero_self_en/spendlog/raw_spendlog_{date}".format(date=date)
            os.system(cmd1)
        elif platform in ['superhero_tw']:
            cmd = "rsync -avzP --partial-dir={tmp_dir} --password-file=/etc/rsyncd_password {username}@{IP}::bi_data/superhero_tw/action_log/*_{date} /home/data/superhero_tw/action_log/".format(
                username=username, IP=IP, date=date, tmp_dir=tmp_dir)
            os.system(cmd)
        elif platform in ['superhero2_tw']:
            cmd = "mv /home/data/superhero2_tw/redis_stats/act* /home/data/superhero2_tw/active_user/ && mv /home/data/superhero2_tw/redis_stats/reg* /home/data/superhero2_tw/new_user/ "
            os.system(cmd)
        elif platform in ['dancer_tw']:
            cmd = "rsync -avzP --partial-dir={tmp_dir} --password-file=/etc/rsyncd_password {username}@{IP}::bi_data/pub_taiwan_gpn/redis_stats/*_{date} /home/data/dancer_tw/redis_stats/".format(
                username=username, IP=IP, date=date, tmp_dir=tmp_dir)
            os.system(cmd)
        else:
            sys.exit()


rsync(platform, username, IP, DATE, port)

