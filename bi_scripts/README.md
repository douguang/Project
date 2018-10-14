# 凯奇谷数据分析代码库说明

## 添加系统路径
需要将根目录加到系统Python目录中。参见：<https://www.douban.com/note/334738164/>，
建议使用方法三。

```sh
$ python -c 'import site; print site.getsitepackages()[0]'
/usr/local/lib/python2.7/site-packages
# 进入该项目根目录下执行以下命令
$ echo `pwd` > /usr/local/lib/python2.7/site-packages/bi.pth
# 使用以下命令查看当前目录是否在输出中
$ python -c 'import sys; print sys.path'
```

```
luigi_python_backup : 备份之前文超写的底层luigi脚本
```
