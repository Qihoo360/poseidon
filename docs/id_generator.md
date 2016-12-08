## 全局docid生成器 

为DocGz提供唯一id编号。
海量数据为减小存储每天都从0重新生成id，为减少压力每次请求可获得一个id区间。  

### HTTP GET 请求参数如下：
 
* count : 请求分配多少个id
* day : 日期，为可选参数，默认为当前日期。格式：YYYYMMDD
* business_name : 业务名

举例：
> * http://domain:port/service/idgenerator?count=135&day=20160229&business_name=test

### 返回值为JSON格式

```json
{
    "errno":0, #错误码
    "errmsg":"", #错误信息
    "start_index":1, #开始索引
    "count":  #返回id数量
    "time":  #服务端当前时间戳 
}
```

* errno定义
> * 0 成功
> * 100 系统错误
> * 101 缺少参数

### 压缩

+ 采用无损压缩算法-[差分](https://zh.wikipedia.org/zh-sg/%E5%B7%AE%E5%88%86%E7%B7%A8%E7%A2%BC)  
+ 查看[build_inverted_index.md](build_inverted_index.md)获取更多信息
