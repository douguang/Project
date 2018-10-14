#!/bin/sh
DATE=$1
if [ ! $DATE ];then
    DATE=`date +%Y-%m-%d --date yesterday`
fi
DATE2=`date -d "$DATE" +%Y_%m_%d`
HMS=`date +%H-%M-%S`
PLATFORM="
qiling_ks
qiling_tx
qiling_ios
"

log_path=/home/data/bi_scripts/luigi_log/luigi_${DATE2}_${HMS}.log
cd /home/data/bi_scripts && \
rm -rf config/qiling_*
echo "[`date +%Y-%m-%d:%H:%M:%S`] $0 start to run!" >> log.txt

for pf in $PLATFORM
do
    /usr/local/bin/python -m luigi --workers 3 --module qiling.luigi_test.uniform_entry QilingDaily --date $DATE --platform $pf 2>&1 | tee ${log_path} && \
    echo 'done'
done

echo "[`date +%Y-%m-%d:%H:%M:%S`] $0 ends!" >> log.txt

