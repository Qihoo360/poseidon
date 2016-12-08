# DocBuilder

------
功能：将需要建索引的文件转换为DocGzHDFSFile及DocGzMeta，上传至hdfs或者本地目录

# 文件命名规范
文件命中必须包含2005-01-02-04这种格式的传，用于识别该文件该入到哪个DocGzHDFSFile

# src_provider
数据源抽象成src_provider,相当于一个队列,不断吐出需要建索引的文件path给DocBuilder,目前支持监控目录、redis-list、也可以根据接口自行实现其他数据源

## 监控目录
将需要生成索引的原始文件都放到指定的文件夹下，src_provider会监控新文件生成，将文件path按顺序吐给DocBuilder

示例配置
```
"src_provider":{
    "src_type":"dm",
    "dm":{
        "monitor_interval_sec":15,
        "pop_last_file":true,
        "time_out_ms":1000,
        "data_dir":"/home/poseidon/data/logto_hdfs",
        "monitor_paths":[
            "/home/poseidon/apps/data1/access",
            "/home/poseidon/apps/data2/access",
            "/home/poseidon/apps/data3/access",
            "/home/poseidon/apps/data4/access"
        ]
    }
}
```

参数：
> * monitor_interval_sec: 多长时间扫描一次目录
> * pop_last_file: 由于最后一个文件可能还没有写完，返回是是否pop最后一个文件
> * time_out_ms: 没有新文件时等待时间
> * data_dir: 记录当前吐出的最新的文件，下一次将只返回比该文件更新的文件
> * monitor_paths: 监控的目录，暂不支持递归

## redis-list
将需要建索引的原始文件path都push到redis-list（right_push），src_provider会将文件path按顺序吐给DocBuider

示例配置
```
"src_provider":{
    "src_type":"redis",
        "redis":{
            "host":"127.0.0.1",
            "port":6500,
            "passwd":"this_is_passwd",
            "key":"file-list"
        }
},
```

参数：
> * key: redis中的key，该key应该是一个redis的list, 里面每一个key都是一个本地文件的path

# 配置
需要如下配置项
```
"hadoop_remote_dir":"/home/poseidon/business_name",
"hadoop_remote_time_dirs":[
    "Y-m-d",
    "H"
],

"flush_minute":20,
"max_merge_file_size":1200000000,
"retry_times":3,
"hadoop_cmd":"/usr/bin/hadoop/bin/hadoop",
"gather_minute":20,
```
> * hadoop_remote_dir: 入到hdfs的目录，不置该项则不上传
> * hadoop_remote_time_dirs: 起名
> * gather_minute：合并文件名中日期接近的文件，减少文件数
> * flush_minute：只有当前时间超过(文件名中的时间 + flush_minute)时才开始对该进行处理，目的是等待该时间的文件基本都生成以后，尽量将其合并为一个文件
