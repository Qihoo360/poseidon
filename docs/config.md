# 配置文件模板说明

docmeta,index,indexmeta的配置文件采用json格式：
参考[test.json](/cloudsafeeng/openposeidon/raw/master/builder/index/src/main/etc/test.json)
        
## common 节点配置项说明
>* *name* : 业务名
>* *data_hashid_section* : token的hashid的映射区间，默认10000000000。
>* *line_per_doc* : 每个doc包含多少行log。
>* *indexgzmeta_section* : InvertedIndexMetaGz区间值，每 *indexgzmeta_section* 组合一个InvertedIndexGZ。
>* *indexgz_file_num_per_field* ： 每种字段分成 *indexgz_file_num_per_field* 个 *InvertedIndexGzHDFSFile* 文件,  
最终所有的InvertedIndexGZ会存放到 *indexgz_file_num_per_field* 个 *InvertedIndexGzHDFSFile* 文件中。
>* *meta_service* : meta服务的地址。
>* *local_mock* : 本地模拟运行hadoop任务，不需要hadoop集群环境。
>* *hdpfs_index_base_path* : 构建索引信息所输出的hdpfs根目录。

## inverted_index 节点配置项说明
>* *save_line_count_per_map*: 一个Token 能关联的 docid 数量的最大值。
>* *hdfs_path* : 日志进过doc组件转换后的格式化日志路径。
>* *etcs* : MR任务所需的依赖配置文件；文件在index-1.0-SNAPSHOT.tar中的etc目录中。
>* *libs* : MR任务所需的jar lib文件；文件在index-1.0-SNAPSHOT.tar中的lib目录中。
>* *data_format* : 日志数据格式，现在支持3种：
>
>> JSON 
>>> tokenizer 中的每个字段必须具有层级的含义，用 . 来分隔层级 ,如“data.sha1”,先找到data节点，再找sha1节点值。
>>
>> TAB 
>>> 每个字段都是按顺序排列,字段间'\t'分隔，字段名需要单独指定 field_names ,字段中不能包含 *_*   
>>
>> KV
>>> 日志每行多个key-value 对；
>>
>
>* *field_names* : *data_format*为TAB时有效，字段名列表。
>* *tokenizer* : 分词器，对哪些字段指定 *分词规则*。注意，某个字段含有中文或者特殊字符时，建议加上urlencode和keyword。
>
>>分词规则
>>
>>> *base64decode*: 
>>> 将字符串base64 decode后输出
>>>
>>> *urldecode*:
>>> 将字符串url decode后输出
>>> 
>>> *urlencode*:
>>> 将字符串url encode后输出
>>>
>>> *keyword*:
>>> 将整个字符串当做一个分词后输出，例如日志里面的mid、md5、sha1等等；字符串中有大写字母的会转成小写字母
>>>
>>> *url*:
>>> 将字符串按url进行分词后输出
>>>
>>> *path*:
>>> 将字符串按path进行分词后输出
>>>
>>> *split*:
>>> 将字符串按指定分隔符切分成多个分词后输出，分隔符支持 正则 表达式，注意在正则表达式中有明确含义的字符，需要用 “\\”转意
>>>
>>> *regexcheck*:
>>> 将字符串进行正则匹配检测，要测试的字符串符合该条件输出，否则丢弃，正则表达式需使用base64编码
>>>
>>> *text*:
>>> 将字符串按普通文本进行分词
>>
>
>* *alias*: 字段别名，没有别名的，value为原字段。例如："text": "text" 或者 "text": "t",




## 待补充


