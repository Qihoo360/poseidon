# HDFS 文件读取

### 打包
`sh build.sh` 生成结果放到dist目录

### 部署
将打包结果解压，执行`sh bin/hdfsReader.sh <start|stop|restart>`脚本。

修改端口号直接修改hdfsReader.sh中的port

**`记得安装crontab`**
