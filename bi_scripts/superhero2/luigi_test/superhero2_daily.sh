#!/bin/sh

DATE=$1
if [ ! $DATE ];then
    DATE=`date +%Y-%m-%d --date yesterday`
fi
DATE2=`date -d "$DATE" +%Y_%m_%d`
HMS=`date +%H-%M-%S`
log_path=/home/data/bi_scripts/luigi_log/luigi_${DATE2}_${HMS}.log
PLATFORM="
superhero2_tw
superhero2
"

cd /home/data/bi_scripts && \
rm -rf config/*
echo "[`date +%Y-%m-%d:%H:%M:%S`] $0 start to run!" >> log.txt

for pf in $PLATFORM
do
    echo "$pf start to run"
    /usr/local/bin/python -m luigi --workers 3 --module superhero2.luigi_test.uniform_entry SuperheroDaily --date $DATE --platform $pf 2>&1 | tee ${log_path} && \
    echo 'done'
done

echo "[`date +%Y-%m-%d:%H:%M:%S`] $0 ends!" >> log.txt

