#!/bin/bash

BI_SCRIPT_DIR=/home/data/bi_scripts
DATE=$1
if [ ! $DATE ];then
    DATE=`date +%Y-%m-%d --date yesterday`
fi
DATE2=`date -d "$DATE" +%Y%m%d`
PLATFORM=`python $BI_SCRIPT_DIR/check_hive_data.py $DATE2 | grep -vE "Warning" | awk -F':' '{print $1}' | sort | uniq`

for pf in $PLATFORM
do
    echo "$pf start to run"
    if [ $pf = "qiling_ks" ]||[ $pf = "qiling_tx" ]||[ $pf = "qiling_ios" ];then
        cd $BI_SCRIPT_DIR
        /usr/local/bin/python -m luigi --workers 3 --module qiling.luigi.uniform_entry QilingDaily --date $DATE --platform $pf
    elif [ $pf = "sanguo_ks" ]||[ $pf = "sanguo_tx" ]||[ $pf = "sanguo_tw" ]||[ $pf = "sanguo_tl" ]||[ $pf = "sanguo_kr" ]||[ $pf = "sanguo_in" ]||[ $pf = "sanguo_ios" ];then
        cd $BI_SCRIPT_DIR
        /usr/local/bin/python -m luigi --workers 3 --module sanguo.luigi.uniform_entry SanguoDaily --date $DATE --platform $pf
    elif [ $pf = "superhero_bi" ]||[ $pf = "superhero_qiku" ]||[ $pf = "superhero_vt" ];then
        cd $BI_SCRIPT_DIR
        /usr/local/bin/python -m luigi --workers 3 --module superhero.luigi.uniform_entry DailyTrigger --date $DATE --platform $pf
    elif [ $pf = "dancer_pub" ]||[ $pf = "dancer_tx" ]||[ $pf = "dancer_tw" ];then
        cd $BI_SCRIPT_DIR
        /usr/local/bin/python -m luigi --workers 3 --module dancer.luigi.uniform_entry DancerDaily --date $DATE --platform $pf
    elif [ $pf = "superhero_self_en" ];then
        cd $BI_SCRIPT_DIR
        /usr/local/bin/python -m luigi --workers 3 --module superhero_self_en.luigi.uniform_entry DailyTrigger --date $DATE --platform $pf
    else
        exit
    fi
done
