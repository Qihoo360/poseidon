# Meta Service

针对存放Meta信息的NoSQL服务提供统一的HTTP接口服务。NoSQL服务目前支持下列类型几种协议类型：

1. memcached
2. redis

备注：这里的NoSQL主要用来存储Poseidon系统中倒排索引数据的 *Meta* 信息和原始数据的 *Meta* 信息。

## 查询数据接口

* 使用HTTP POST请求
* URI
    * /service/meta/business/doc/get : 查询 *business* 业务 *DocGz* 的 *Meta* 信息
    * /service/meta/business/index/get :  查询 *business* 业务 *InvertedIndexGz* 的 *Meta* 信息 
- URI 上有一个参数 *business_name* ，用于表示不同的业务。这里可以实现不同业务数据，存放到不同的NoSQL实例中。
- 请求body

  key\nkey\nkey

  说明：可以同时一次性批量查询多个key，用\n分隔。也就是一行一个key

- 返回body

  key\tbase64_encode(val)\n
  
  key\tbase64_encode(val)\n
  
  key\tbase64_encode(val)\n
  
  说明:val表示从NoSQL中查询出的值，base64编码，用\t分隔；一行一个结果。

## 更新数据接口

* URI
    * /service/meta/business/doc/set : 更新 *business* 业务 *DocGz* 的 *Meta* 信息
    * /service/meta/business/index/set :  更新 *business* 业务 *InvertedIndexGz* 的 *Meta* 信息 
- URI 上有一个参数 *business_name* ，用于表示不同的业务。这里可以实现不同业务数据，存放到不同的NoSQL实例中。
- 使用HTTP POST请求
- 请求body的报文格式如下：

  key\tbase64_encode(val)\n

  key\tbase64_encode(val)\n

  key\tbase64_encode(val)\n

  说明:val表示是需要更新NoSQL中的值，base64编码，用\t分隔；一行一个Key/Value对

- 返回body

  key\tOK\n
  
  key\terr\n
  
  说明:每行是一个key的插入结果。
  
  OK表示插入成功；
  
  非OK表示插入失败，并将具体的错误信息一起返回