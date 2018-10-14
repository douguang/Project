#!/bin/sh

ip_list="123.206.77.23 120.92.2.239 118.186.70.86 118.193.27.58" # 金山和腾讯管理机

basedir=$(dirname "$0")
echo ${basedir}
for ip in $ip_list
do
    echo "release to ${ip}"
    rsync --progress ${basedir}/* admin@${ip}:/data/bi_script_new
done
