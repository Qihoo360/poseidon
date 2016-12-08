# 波塞冬：Poseidon

波塞冬，是希腊神话中的海神，在这里是寓意着海量数据的主宰者。

Poseidon 系统是一个日志搜索平台，可以在数百万亿条、数百PB大小的日志数据中快速分析和检索特定字符串。
360公司是一个安全公司，在追踪 APT（高级持续威胁）事件时，经常需要在海量的历史日志数据中检索某些信息，
例如某个恶意样本在某个时间段内的活动情况。在 Poseidon 系统出现之前，都是写 Map/Reduce 计算任务在 Hadoop 集群中做计算，
一次任务所需的计算时间从数小时到数天不等，大大制约了 APT 事件的追踪效率。
Poseidon 系统就是为了解决这个需求，能在几秒钟内从数百万亿条规模的数据集中找出我们需要的数据，大大提高工作效率；
同时，这些数据不需要额外存储，仍然存放在Hadoop集群中，节省了大量存储和计算资源。该系统可以应用于任何结构化或非结构化海量(从万亿到千万亿规模)数据的查询检索需求。

# [Quick Start](docs/get_started.md)

# 所用技术

- 倒排索引：构建日志搜索引擎的核心技术
- Hadoop：用于存放原始数据和索引数据，并用来运行Map/Reduce程序来构建索引
- Java：构建索引时是用Java开发的Map/Reduce程序
- Golang：检索程序是用Golang开发的
- Redis/Memcached：用于存储 *Meta* 元数据信息


# 目录结构

### builder

这里存放的是数据生成工具

- doc ：将原始日志转换为Poseidon格式的数据。
- docmeta ：将Doc相关的元数据信息写入NoSQL库中的工具。
- index ：从原始日志生成倒排索引数据的程序工具，是Hadoop 的 Map/Reduce 作业程序。
- indexmeta ：将倒排索引的元数据写入NoSQL库中的工具。

### common

目前仅仅用来存放该项目中用到的 `protobuf` 定义

### docs 

存放了相关的技术文档。

* 项目设计文档
    * [设计思路和原理(2016上海QCon大会分享PPT)](docs/design_detail.pdf)
    * [如何构建倒排索引](docs/build_inverted_index.md)
    * [术语解释](docs/component.md)
    * [构建倒排索引时所需的配置文件模板的说明](docs/config.md)
    * [快速开始](docs/get_started.md)
* 微服务
    * [HDFS数据读取微服务 hdfs_reader](docs/hdfs_reader.md)
    * [ID生成中心微服务 id_generator](docs/id_generator.md)
    * [元数据存取微服务 meta](docs/meta.md)
    * [核心搜索引擎服务 searcher](docs/searcher.md)
    * [搜索引擎代理服务 proxy](docs/proxy.md)


### service

这里存放的是各个HTTP微服务服务的程序

* [hdfsreader](docs/hdfs_reader.md) ：读取HDFS中某个文件路径的一段数据。 
    * /service/hdfsreader
* [idgenerator](docs/id_generator.md) ：全局的ID生成中心
    * /service/idgenerator
* [meta](docs/meta.md) ：针对存放Meta信息的NoSQL提供统一的HTTP接口服务
    * /service/meta/business/doc/get : DocGzMeta 信息查询接口
	* /service/meta/business/doc/set : DocGzMeta 信息更新接口
    * /service/meta/business/index/get : InvertedIndexGzMeta 信息查询接口
	* /service/meta/business/index/set : InvertedIndexGzMeta 信息更新接口
* [searcher](docs/searcher.md) ：Poseidon搜索引擎的核心检索服务
* [proxy](docs/proxy.md) ：searcher的一个代理，并能实现跨时间的查询服务
* allinone ： 为简化部署，将 idgenerator/meta/searcher/proxy 四个微服务集成在一个进程中，提供统一的服务接口


