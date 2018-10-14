# 文件说明

该文件夹为服务器端生成数据用的脚本。

可以在本地修改，通过 `release.sh` 脚本发布到服务器端。

每个平台的管理机上有一个定时任务，形如：

```
10 0 * * * /bin/sh /data/bi_script_new/get_all_bi_data.sh pub_js
```
