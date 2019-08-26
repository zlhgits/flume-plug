使用flume抓取数据库表的数据封装成json，发送到channel

Flume-ng安装
  解压安装包，将jsonfordb程序打包好即jsonfordb-1.0-SNAPSHOT.jar，
  拷贝到解压后的flume-ng/lib中（flume-ng压缩包中已有该jar，如jsonfordb重新开发了，则打包替换）。

Conf配置：
Linux系统
将flume-env.sh.template 复制一份为flume-env.sh
然后将flume-env.sh第22行打开修改为本机JDK地址
windos系统
将flume-env.ps1.template 复制一份为flume-env.ps1

启动命令：
1、bin\flume-ng.cmd agent --conf conf --conf-file conf\xxxxx.conf --name xxx
2、bin/flume-ng agent --conf conf --conf-file conf/xxxx.conf --name xxx -Dflume.root.logger=INFO,console >> /root/xxxx.log 2>&1 &