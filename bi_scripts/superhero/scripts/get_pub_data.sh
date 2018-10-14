#!/bin/sh
set -e  # 任意脚本出错均直接退出

DATE=date
FORMAT="%Y%m%d"
# 开始补数据日期
start=`$DATE +$FORMAT -d "20160619"`
# 补数据结束日期
end=`$DATE +$FORMAT -d "20160622"`
now=$start


while [[ "$now" < "$end" ]] ; do
  echo "$now"
  # 数据拉取
  sh /home/data/script/get_superhero.sh $now;
  # 执行生成每日数据
  sh /home/data/superhero/bi_tool/total.sh $now;
  # 将数据导入 web 所在文件夹
  sh /home/hadoop/godvs/scripts/scripts_pub/online_data_scripts/get_all_data_pack_scripts.sh $now;
  now=`$DATE +$FORMAT -d "$now + 1 day"`
done

# 将数据导入 web 系统中
sh /home/hadoop/godvs/scripts/scripts_pub/online_data_scripts/load_normal_data_scripts.sh;

echo 'done'
curl 'https://hook.bearychat.com/=bw79O/incoming/0f79b0211fcffac6e577da80fbbe928a' -X POST -d 'payload={"text":"超级英雄pub web后台数据已补完"}'
