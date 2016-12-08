## Proxy

*Written By Go Language*  

将多天/多关键字查询分发给不同searcher,并发处理请求,收集数据,进一步处理数据,返回给客户端

### Build  
```bash
cd PROXYPATH && sh ./build.sh  #在bin目录下生成编译文件,并更新$ROOT/dist
```  

### Run
应用程序运行控制脚本在 *serverctl* 脚本中
```bash
cd PROXYPATH && sh ./serverctl (start|restart|stop)  # @todo reload 
```

### 环境初始化  
1.根据机房初始化配置文件  
2.初始化日志目录  
3.修改相关目录读写权限  
别担心,上线时,上线脚本会自动执行 *sh ./serverctl envinit* 来自动完成环境的初始化


### Deploy 
所有的上线部署脚本都在 *$GOPATH/tools* 目录下  
在 *conf/conf.sh* 中配置测试集群和线上集群,每台server按照回车符分割
```bash
online_cluster=""
beta_cluster=""
```  
你的开发机要开通与线上机器的ssh信任，要上线使用的用户名在上线脚本中配置  
*conf/conf.sh*    
```bash
# 部署使用的账号  
SSH_USER=""
```
脚本会先把要上线的文件拷贝到线上服务器 然后自动 执行 *sh ./serverctl restart*  重启服务，注意这个过程不是热重启，会丢掉部分请求

```bash
cd PROXYPATH && sh ./tools/deploy-beta.sh    file       #上线测试环境
cd PROXYPATH && sh ./tools/deploy-package.sh            #第一次上线
cd PROXYPATH && sh ./tools/deploy-release.sh file       #第一次上线后其他上线单独的配置文件或者应用程序  
```  

### Logs  
你可以通过conf目录下的 *app.conf.base* 配置日志的错误级别和日志路径,日志根路径默认放在项目根目录下logs目录,日志按天自动分割   
```bash
#日志文件根目录
log_root = ../logs

#日志level, 0:debug 1:access 2:warning 3:error
log_level = 0
```