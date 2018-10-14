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
        if [ $pf = "superhero_mul" ];then
            cd $BI_SCRIPT_DIR
            /usr/local/bin/python -m luigi --workers 3 --module superhero.luigi_test.uniform_entry SuperheroDaily --date $DATE --platform $pf
        else
            continue
        fi
    done
fi

if [ $platform ];then
    echo "$platform start to run"
    if [ $platform = "superhero_mul" ];then
        cd $BI_SCRIPT_DIR
        if [ -e $BI_SCRIPT_DIR/history_task_test/$platform/$DATE/$task_name ];then
            rm -f history_task_test/$platform/$DATE/$task_name
        fi
        /usr/local/bin/python -m luigi --workers 3 --module superhero.luigi_test.uniform_entry SuperheroDaily --date $DATE --platform $platform
    else
        exit
    fi
fi

