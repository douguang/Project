#!/bin/sh

echo "开始执行：$0"

platform=$1
if [[ $platform == 'pub_js' ]];then
    echo '金山平台'
    ip=`cat /data/bi_script_new/app_ip_list_ks`
elif [[ $platform == 'pub_tencent' ]]; then
    echo '腾讯平台'
    ip=`cat /data/bi_script_new/app_ip_list_tx`
elif [[ $platform == 'pub_ios' ]]; then
    echo '金山ios平台'
    ip=`cat /data/bi_script_new/app_ip_list_ios`
elif [[ $platform == 'pub_tw_gpn' ]]; then
    echo '台湾平台'
    ip=`cat /data/bi_script_new/app_ip_list_tw`
else
    echo 'platform 参数错误'
    exit 1
fi

date_y=$2
if [ ! ${date_y} ];then
    date_y=`date +%Y%m%d --date '1 days ago'`
fi

echo ${date_y}

dest_dir=/data/action_log_mid/${platform}/${date_y}

for i in ${ip}
do
    printf "\n正在传输：$i \n\n"
    mkdir -p ${dest_dir}/${i}
    /usr/bin/rsync -zutrvP admin@${i}:/data/sites/backend/logs/dena_dmp/*_${date_y}.log "${dest_dir}/${i}/"
done

cat ${dest_dir}/*/*_${date_y}.log > /data/bi_data/${platform}/action_log/action_log_${date_y}_tmp && \
mv /data/bi_data/${platform}/action_log/action_log_${date_y}_tmp /data/bi_data/${platform}/action_log/action_log_${date_y} && \
# rm -rf /data/action_log_mid/${platform}/* && \
echo 'done'


# if [ $? -eq 0 ]; then
#    rm -rf *_${date_y}.log
# else
#     exit 1
# fi
