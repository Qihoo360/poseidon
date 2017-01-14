
## 准备工作

### 开发环境依赖
我们的生产环境和测试环境均是64位Liunx,快速开始建议也在同样的环境中测试。
```bash
cat /etc/redhat-release
CentOS release 6.2 (Final)
```

如果你的机器上没有以下依赖环境，请务必安装；已经安装过的，可以跳过；

-  [Java jdk]: 1.7+ 
-  [Golang]: 1.6+ 
-  [Protobuf]: 3.0+  可选
-  [Gradle]:    
下载gradle的二进制包，解压到某个目录中，建议是$HOME，将gradle的bin目录设置到$PATH中：
    vi ~/.bashrc 或者~/.bash_profile：
    /Users/liwei/Developer/src/github.com/qihoo360/poseidon/docs/get_started.md
        GOPATH=$HOME
        export $GOPATH
        GRADLE_HOME=/home/xxxxx/gradle-3.1
        PATH=$PATH:$HOME/bin:$GRADLE_HOME/bin
        export $PATH
    保存退出后执行： 
       
        source  ~/.bashrc
        #source ~/.bash_profile
        
             
-  [Memcache]: 1.4.4+ 
-  [Redis]: 2.4+


## 编译&打包
### 下载代码：
1. 进入您的工作目录(建议以`$HOME`目录作为您的工作目录)，将当前这个目录添加到`$GOPATH`环境变量中，然后执行如下命令:    
     
        mkdir -p src/github.com/Qihoo360
        cd src/github.com/Qihoo360
        git clone https://github.com/Qihoo360/poseidon
        cd poseidon     
    
    
2. 执行以下命令，产生可运行的包输出到 `dist` 目录

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
> 注意，执行 `sh bin/demo.sh FILE` 之后，需要查看/home/poseidon/src/test/YYYY-MM-DD/中是否有新的文件生成；
> YYYY-MM-DD ： 开始索引任务的日期，比如2016-10-10；YYYY-MM-DD从`demo.sh`运行输出中获取；
>若没有文件生成，需要等待直到文件生成；

    ls -ahtl /home/poseidon/src//test/YYYY-MM-DD/
    
 
#### 4.3 docmeta,index,indexmeta :部署和构建一个索引文件demo:
> 执行下列命令步骤前，必须确保`meta`服务先部署运行，否则后续构建索引任务会失败

    tar -xvf index-0.1.tar
    cd index-0.1
    /bin/bash bin/mock_start.sh  YYYY-MM-DD


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

[Java jdk]:http://www.oracle.com/technetwork/java/javase/downloads/index.html
[Golang]:https://golang.org/dl/
[Protobuf]:https://developers.google.com/protocol-buffers/docs/downloads
[Gradle]:https://gradle.org/gradle-download/
