## 准备工作
  首先得有个hadoop系统； 
  如果没有可用的hadoop集群系统，可以用docker快速搭建一个hadoop集群系统：
      
      参考https://hub.docker.com/r/sequenceiq/hadoop-docker/镜像
      直接下载镜像：
      docker pull sequenceiq/hadoop-docker
      docker pull sequenceiq/hadoop-docker:2.7.1
      docker run -it sequenceiq/hadoop-docker:2.7.1 /etc/bootstrap.sh -bash
      
### [开发环境依赖](get_started.md)      

## 编译&打包
### 下载代码：
1. 进入您的工作目录(建议以`$HOME`目录作为您的工作目录)，将当前这个目录添加到`$GOPATH`环境变量中，然后执行如下命令:    
     
        mkdir -p src/github.com/Qihoo360
        cd src/github.com/Qihoo360
        git clone https://github.com/Qihoo360/poseidon
        cd poseidon     
    
    
2. 执行以下命令，产生可运行的包输出到 `dist` 目录
> 编译之前，请指定所依赖的hadoop库版本号：
> 修改builder/index/build.gradle中的`hadoopVersion`,以实际的hadoop系统为准；本例中为"2.7.1"；

> 修改service/hdfsreader/build.gradle中hadoop相关的version；本例中为"2.7.1"；
> 修改service/hdfsreader/src/main/etc/core-site.xml中的fs.defaultFS的value，将默认的`file:/`修改为正确的hdfs配置，hadoop默认应该修改为`hdfs://localhost:9000`

> 修改builder/docformat/etc/docformat/docformat.json中的hadoop_cmd，将默认的`./bin/local-hadoop.sh`修改为`./bin/real-hadoop.sh`
> 修改builder/docformat/script/common/real-hadoop.sh中的hadoop命令为正确路径，本例中是`/usr/local/hadoop/bin/hadoop`

    sh ./build.sh

### dist：

    index-0.1.tar
    proxy-0.1.tar.gz
    searcher-0.1.tar.gz
    idgenerator-0.1.tar.gz
    meta-0.1.tar.gz
    docformat-0.1.tar.gz
    hdfsReader-0.1.tar


## 部署

### 下面部署示例，以业务名为`test`为例
> 为避免出错，请严格按照以下步骤顺序执行

### 1. 准备工作

* 在本地启动`memcached`进程，端口为 *11211*
    * `meta`使用了memcached, 如果memcached服务地址与上述不一样, 请修改`meta/conf/test.ini`
* 在本地启动`redis`进程，地址: `127.0.0.1:6379`, 密码：`password` redis必须以`ip:port`端口启动，且必须设置密码 
    * `idgenerator`服务使用了redis，如果redis服务地址和密码与上述不一样，请修改 `idgenerator/conf/idgenerator.ini` 

### 2. 安装service

#### idgenerator,proxy,searcher,meta
`cd dist`, 然后执行下面脚本 

```bash
ROOT=`pwd`
for f in idgenerator proxy searcher meta
do
    cd $ROOT
    tar -zxvf ${f}*.tar.gz
    cd $f && sh serverctl start

done
```


#### 3. 安装hdfsReader

```bash
tar -xvf hdfsReader.tar
cd hdfsReader
sh bin/hdfsReader.sh start
```

### 4. builder

#### 4.1 docformat 部署

        //TODO 
        mkdir -p /home/poseidon/data/src
        chmod 777 -R /home/poseidon
        tar -zxvf docformat-0.1.tar.gz
        cd docformat-0.1
        sh bin/install.sh stop # 停服务，如果之前已经启动过
        rm -rfv /home/poseidon/data/* # 清空数据源
        sh bin/install.sh start
        
        #下载示例日志文件
        wget https://raw.githubusercontent.com/liwei-ch/poseidonlog/master/weibo_data.tar.gz
        tar -xvf weibo_data.tar.gz
        sh bin/demo.sh weibo_data.txt
        


#### 4.2 等待docformat日志完成
> 注意，执行 `sh bin/demo.sh FILE` 之后，需要查看hdfs:/home/poseidon/src/test/YYYY-MM-DD/中是否有新的文件生成；//TODO  
> YYYY-MM-DD ： 开始索引任务的日期，比如2016-10-10；YYYY-MM-DD从`demo.sh`运行输出中获取；
>若没有文件生成，需要等待直到文件生成；

    hadoop fs -ls /home/poseidon/src//test/YYYY-MM-DD/
    
 
#### 4.3 docmeta,index,indexmeta :部署和构建一个索引文件demo:
> 执行下列命令步骤前，必须确保`meta`服务先部署运行，否则后续构建索引任务会失败

    tar -xvf index-0.1.tar
    cd index-0.1
    
> 指定hadoop系统的namenode： 
> 修改etc/test_hdfs.json中的`name_node`，值格式为"host:port"，修改为实际的namenode地址； 
> 本例中为127.0.0.1:9000；  
    
    
    /bin/bash bin/start.sh  YYYY-MM-DD  //注意：不是mock_start.sh
    
### 5. 测试

索引成功生成之后，访问poseidon_proxy代理查询数据 `sh test.sh` 

##### 查询接口说明 

```bash
curl -XPOST 'http://127.0.0.1:39460/service/proxy/mdsearch' -d '{
  "query":{
	  "page_size":100,"page_number":0,"days":["YYYY-MM-DD"],"business":"test","options":{
		  "pv_only":0,"filter":""
	  },
		  "keywords":{
			  "text":"3599021455585075"
		  }
  }
}
'

参数说明
page_size : 每页展示条数
page_number : 页码
days : 查询天数,时间格式：YYYY-MM-DD，例如2016-12-08, 数组格式多天用英文逗号分割
business : 业务名称，默认test
options : 其他参数，pv_only是否只查询pv，filter过滤字段
keywords : 要查询的关键词，json键值对格式,当传多个key时是and关系查询,查询汉字需要`urlencode并转小写`
```
