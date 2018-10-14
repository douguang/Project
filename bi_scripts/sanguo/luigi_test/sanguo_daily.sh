#!/bin/sh

DATE=$1
if [ ! $DATE ];then
    DATE=`date +%Y-%m-%d --date yesterday`
fi
DATE2=`date -d "$DATE" +%Y_%m_%d`
HMS=`date +%H-%M-%S`
log_path=/home/data/bi_scripts/luigi_log/luigi_${DATE2}_${HMS}.log
PLATFORM="
sanguo_ios
sanguo_ks
sanguo_tt
sanguo_tl
sanguo_tx
sanguo_tw
sanguo_kr
sanguo_mth
sanguo_bt
sanguo_guandu
sanguo_chaov
"

cd /home/data/bi_scripts && \
rm -rf config/sanguo_*
echo "[`date +%Y-%m-%d:%H:%M:%S`] $0 start to run!" >> log.txt

for pf in $PLATFORM
do
    echo "$pf start to run"
    test=$(/usr/local/bin/python -m luigi --workers 3 --module sanguo.luigi_test.uniform_entry SanguoDaily --date $DATE --platform $pf)
    result=$(echo "${test}" |grep ERROR)
    task_name=$(echo "${result}" |grep ERROR|cut -d '|' -f 2|cut -d '=' -f 9|cut -d ')' -f 1)
    echo '------'
    echo $result
    echo $task_name
    if [[ "$result" != "" ]]
    then
        echo $test  | tee ${log_path} && curl 'https://oapi.dingtalk.com/robot/send?access_token=aa9ef63e6a4aa77438c05552a8ec82024fdff1dadd3f4e29622e028c200345ac' -H 'Content-Type: application/json' -d '{"msgtype": "text","text": {"content":"'`date +%Y-%m-%d:%H:%M:%S`'-['$DATE']-['$pf']-['$task_name']=['False'] "} }' && echo 'done'
    else
        echo $test  | tee ${log_path} && echo 'done'
    fi
done

echo "[`date +%Y-%m-%d:%H:%M:%S`] $0 ends!" >> log.txt


