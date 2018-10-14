#!/bin/sh
log_path=/home/data/bi_scripts/luigi_log/luigi_`date +"%Y_%m_%d_%H_%M_%S"`.log

cd /home/data/bi_scripts && \
rm -rf history_task/superhero2 config && \
echo "[`date +%Y-%m-%d:%H:%M:%S`] $0 start to run!" >> log.txt && \
/usr/local/bin/python -m luigi --workers 3 --module superhero2.luigi.uniform_entry DailyTrigger > ${log_path} 2>&1 && \
echo 'done' || \
curl 'https://hook.bearychat.com/=bw79O/incoming/0f79b0211fcffac6e577da80fbbe928a' -X POST -d "payload={\"text\":\"【`date +%Y-%m-%d:%H:%M:%S`】【hadoop@192.168.1.27】luigi任务 $0 执行失败\"}"

echo "[`date +%Y-%m-%d:%H:%M:%S`] $0 ends!" >> log.txt

