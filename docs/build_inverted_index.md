# 倒排索引的构建


## map阶段
*   针对每一行日志进行分词处理，按 *field* 对应的分词规则，产生 *Token* 相关的数据
    
*   map输出，字段间'\t'分隔：
        *TokenHashid* *Token* *Filed* *Docid* *Offsize* *PV*  
        
*   *HashId* : token的hash值;Hash算法推荐使用 [murmur3算法](https://en.wikipedia.org/wiki/MurmurHash)  
        
        HashId = murmur3_hash64(Token)
        
*   *TokenId* ：Token的Id，先算出HashId，然后模200取余数；  同一个TokenId，可能会对应多个Token；
    注意TokenId要使用8字节0补充对齐格式输出，例如 HashId=123 那么输出TokenId应该为 00000123  
        
        TokenId = HashId % 200
        
    输出TokenId的C++代码为： 
        
        std::cout << std::setfill ('0') << std::setw (8) << TokenId; 
        
    这是因为hadoop的MR中间排序算法默认是按照字典序排序的，HashId的排序却需要按照数字大小排序
    
*   *FileId*  ： *InvertedIndexGzHDFSFile* 的Id;
    *FileId* 也是 *InvertedIndexGzHDFSFile* 名字的一部分，查找token的IndexMeata数据时能推导出 *InvertedIndexGzHDFSFile* 具体路径；
    *InvertedIndexGzHDFSFile* 具体见下文。
    
    *TokenId* 与 *FileId* 关系如下：
        
        FileId = TokenId / 1000

## map combine阶段
*   将相同分词的docid在本地进行差分合并列表
*   输出：
        *TokenHashId* *Token* *Filed*  *DocList* *PV*
字段间'\t'分隔
*   docid采用差分压缩,  
        
        docid1=raw_docid1  
        docid2=raw_docid2-raw_docid1  
        docid3=raw_docid3-raw_docid2  
        ...  
        docidn=raw_docidn-raw_docid(n-1)  

*   *DocidList*:  
        
        docid1,offsize;docid2,offsize;....;docidn,offsize


## reduce阶段
* reduce产生3份输出：
    *InvertedIndexGzHDFSFile*， 
    *InvertedIndexGzMetaHDFSFile*，
    *middle*
    
* *InvertedIndexGzHDFSFile* 由多个 *InvertedIndexGz* 合并而成
    *InvertedIndexGzHDFSFile* 分成1000个文件，对应1000个文件桶，桶编号0-999，FileID即文件桶的ID。这里的1000个文件，实际上就是Map/Reduce任务执行中Reduce的任务个数。
    逻辑上就实现了将 *InvertedIndexGz* 数据散列分桶存放，每个桶存放一部分 *InvertedIndexGz* 的数据。
    
* *InvertedIndexGz* 算法，N先暂定取值为200(这个取值下面，每个 *InvertedIndexGz* 大小为40KB左右)：
    
        HashId在 [0,N) 之间的组合为一个 InvertedIndexGz
        HashId在 [N,2N) 之间的组合为一个 InvertedIndexGz
        HashId在 [2N,3N) 之间的组合为一个 InvertedIndexGz
        
        依次类推
        
* *Token* 关联的 *docid* 数量应该有一个最大值( *save_line_count_per_map* )，以防数据集太大导致一个 *InvertedIndexGz* 太大，查询时一次性读取太大的数据会导致性能急剧下降。
    最大值暂定为100万，每个 *docid* 预计占用3字节，100万 *docid* 就是 3MB，对应的 *InvertedIndexGz* 是经过压缩的，大约不会超过 1MB 。 
    如果某个 *Token* 关联的 *docid* 数大于最大值，也只取100万，这里会导致某些数据查不到想要的结果。
    

* *InvertedIndexGzMetaHDFSFile*
    该文件是存储 *InvertedIndexGzHDFSFile* 文件的 *Meta* 信息，即 *InvertedIndexGz* 对应的元数据。
    每一个 *InvertedIndexGz* 对应 *Meta* 信息包含： TokenId、FileId、Offset、Length。该文件每行就存储了一个 *InvertedIndexGz* 对应的元数据信息，4列存储，以 *\t* 分割，分别如下：
        
        TokenId FileId  Offset  Length
        
    举例如下
    /the/path/to/data/business/index/2016-08-24/m5gzmeta/part-00977.gz   
        
        00003113       	00977  	0      	317
        00009081       	00977  	317    	78
        00015309       	00977  	395    	54
        00015642       	00977  	449    	59
        00016038       	00977  	508    	285
        00030500       	00977  	793    	110
        00031798       	00977  	903    	69
        00051895       	00977  	972    	80
  
    *InvertedIndexGzMeta* 数据最终会存放到NoSQL中，
    写一个MR程序直接读取 *InvertedIndexGzMetaHDFSFile* 就可以写入到NoSQL了
    表空间可以命名为：/业务名/索引名，例如： /test/text
    key是 TokenId；
    value是 *InvertedIndexGz* 的 *Meta* 信息。其中只要 offset、length 这两个字段即可，hdfs_path、FileId这两个字段可以根据规则推算出来。
