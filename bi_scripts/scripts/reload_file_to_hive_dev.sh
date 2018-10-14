#!/bin/bash

BI_SCRIPT_DIR=/home/data/bi_scripts
DATE=$1
if [ ! $DATE ];then
    DATE=`date +%Y-%m-%d --date yesterday`
fi
DATE2=`date -d "$DATE" +%Y%m%d`

(ps aux | grep Daily) &> /dev/null
if [ $? == 0 ];then
    ps aux | grep Daily | grep -v grep | awk '{print $2}' | xargs kill -9
fi

platform=$2
task_name=$3
PLATFORM_File=""
PLATFORM_Task=""
if [ ! $platform ];then
    PLATFORM_File=`python $BI_SCRIPT_DIR/scripts/check_hive_data_dev.py $DATE2 | grep -E "File" | grep -vE "parse" | awk -F':' '{print $1}' | sort | uniq`
    PLATFORM_Warning=`python $BI_SCRIPT_DIR/scripts/check_hive_data_dev.py $DATE2 | grep -E "Warning" | awk -F':' '{print $1}' | sort | uniq`
    PLATFORM_Average=`python $BI_SCRIPT_DIR/scripts/check_hive_data_dev.py $DATE2 | grep -E "Average" | awk -F':' '{print $1}' | sort | uniq`
    PLATFORM_Task=`python $BI_SCRIPT_DIR/scripts/check_hive_data_dev.py $DATE2 | grep -E "Task|run" | awk -F':' '{print $1}' | sort | uniq`
fi


if [ "x$PLATFORM_File" != "x" ];then
    for pf in $PLATFORM_File
    do
        /usr/local/bin/python /home/data/bi_scripts/scripts/rsync_bi_data.py $pf
    done
fi

if [ "x$PLATFORM_Task" == "x" ];then
    break
else
    for pf in $PLATFORM_Task
    do
        echo "$pf start to run"
        if [ $pf = "sanguo_ks" ]||[ $pf = "sanguo_tt" ]||[ $pf = "sanguo_tx" ]||[ $pf = "sanguo_tl" ]||[ $pf = "sanguo_tw" ]||[ $pf = "sanguo_kr" ]||[ $pf = "sanguo_ios" ]||[ $pf = "sanguo_mth" ];then
            cd $BI_SCRIPT_DIR
            /usr/local/bin/python -m luigi --workers 3 --module sanguo.luigi_test.uniform_entry SanguoDaily --date $DATE --platform $pf
        elif [ $pf = "superhero_bi" ]||[ $pf = "superhero_qiku" ]||[ $pf = "superhero_vt" ]||[ $pf = "superhero_tw" ]||[ $pf = "superhero_self_en" ];then
            cd $BI_SCRIPT_DIR
            /usr/local/bin/python -m luigi --workers 3 --module superhero.luigi_test.uniform_entry SuperheroDaily --date $DATE --platform $pf
        elif [ $pf = "dancer_pub" ]||[ $pf = "dancer_tx" ]||[ $pf = "dancer_tw" ]||[ $pf = "dancer_mul" ]||[ $pf = "dancer_kr" ]||[ $pf = "dancer_cgame" ];then
            cd $BI_SCRIPT_DIR
            /usr/local/bin/python -m luigi --workers 3 --module dancer.luigi_test.uniform_entry DancerDaily --date $DATE --platform $pf
        elif [ $pf = "superhero2_tw" ];then
            cd $BI_SCRIPT_DIR
            /usr/local/bin/python -m luigi --workers 3 --module superhero2.luigi_test.uniform_entry SuperheroDaily --date $DATE --platform $pf
        elif [ $pf = "jianniang_tw" ];then
            cd $BI_SCRIPT_DIR
            /usr/local/bin/python -m luigi --workers 3 --module jianniang.luigi_test.uniform_entry JianNiangDaily --date $DATE --platform $pf
        elif [ $pf = "slg_mul" ];then
            cd $BI_SCRIPT_DIR
            /usr/local/bin/python -m luigi --workers 3 --module SLG.luigi_test.uniform_entry SLGDaily --date $DATE --platform $pf
        else
            continue
        fi
    done
fi

if [ $platform ];then
    echo "$platform start to run"
    if [ $platform = "sanguo_ks" ]||[ $platform = "sanguo_tt" ]||[ $platform = "sanguo_tx" ]||[ $platform = "sanguo_tl" ]||[ $platform = "sanguo_tw" ]||[ $platform = "sanguo_tl" ]||[ $platform = "sanguo_kr" ]||[ $platform = "sanguo_ios" ];then
        cd $BI_SCRIPT_DIR
        if [ -e $BI_SCRIPT_DIR/history_task_test/$platform/$DATE/$task_name ];then
            rm -f history_task_test/$platform/$DATE/$task_name
        fi
        /usr/local/bin/python -m luigi --workers 3 --module sanguo.luigi_test.uniform_entry SanguoDaily --date $DATE --platform $platform
    elif [ $platform = "superhero_bi" ]||[ $platform = "superhero_qiku" ]||[ $platform = "superhero_vt" ]||[ $platform = "superhero_tw" ];then
        cd $BI_SCRIPT_DIR
        if [ -e $BI_SCRIPT_DIR/history_task_test/$platform/$DATE/$task_name ];then
            rm -f history_task_test/$platform/$DATE/$task_name
        fi
        /usr/local/bin/python -m luigi --workers 3 --module superhero.luigi_test.uniform_entry SuperheroDaily --date $DATE --platform $platform
    elif [ $platform = "dancer_pub" ]||[ $platform = "dancer_tx" ]||[ $platform = "dancer_tw" ]||[ $pf = "dancer_mul" ]||[ $pf = "dancer_kr" ]||[ $pf = "dancer_cgame" ];then
        cd $BI_SCRIPT_DIR
        if [ -e $BI_SCRIPT_DIR/history_task_test/$platform/$DATE/$task_name ];then
            rm -f history_task_test/$platform/$DATE/$task_name
        fi
        /usr/local/bin/python -m luigi --workers 3 --module dancer.luigi_test.uniform_entry DancerDaily --date $DATE --platform $platform
    elif [ $platform = "superhero2_tw" ];then
        cd $BI_SCRIPT_DIR
        if [ -e $BI_SCRIPT_DIR/history_task_test/$platform/$DATE/$task_name ];then
            rm -f history_task_test/$platform/$DATE/$task_name
        fi
        /usr/local/bin/python -m luigi --workers 3 --module superhero2.luigi_test.uniform_entry SuperheroDaily --date $DATE --platform $platform
    elif [ $platform = "jianniang_tw" ];then
        cd $BI_SCRIPT_DIR
        if [ -e $BI_SCRIPT_DIR/history_task_test/$platform/$DATE/$task_name ];then
            rm -f history_task_test/$platform/$DATE/$task_name
        fi
        /usr/local/bin/python -m luigi --workers 3 --module jianniang.luigi_test.uniform_entry JianNiangDaily --date $DATE --platform $platform
    elif [ $platform = "slg_mul" ];then
        cd $BI_SCRIPT_DIR
        if [ -e $BI_SCRIPT_DIR/history_task_test/$platform/$DATE/$task_name ];then
            rm -f history_task_test/$platform/$DATE/$task_name
        fi
        /usr/local/bin/python -m luigi --workers 3 --module SLG.luigi_test.uniform_entry SLGDaily --date $DATE --platform $platform
    else
        exit
    fi
fi

