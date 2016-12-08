# 术语解释
*   *Log* ：原始日志。
    
*   *Doc* ：Document的缩写，称为文档。
    是128条（条目数可以配置）原始日志的集合，仍然是明文、按行排列的原始日志数据。
    类似于搜索引擎中的 *文档* 。 
    
*   *DocGz* ：Doc的gz压缩数据。
    
*   *Token* ：一个分词。
    按照一定的分词算法进行分词分出来的单个元素。
    例如一个汉语词、或一个英文单词、或一个MD5串、或一个文件名，等等。
    
*   *Field* ：分词的字段类型。字段类型一般结合业务自行定义。
    搜索的时候选择某种字段类型去搜索某个分词的日志信息。
    
*   *RawLogHDFSFile* ： 存在于hdfs中的原始日志文件，一般是压缩格式。
    
*   *DocGzHDFSFile* ：存在于hdfs中的日志文件，该文件是由一组 *DocGz* 直接拼接在一起的文件。
    由于gz格式特性，该文件仍然可以直接通过gunzip解压。
    
*   *DocGzMeta* ： *DocGz* 的元数据信息，包含下列三个字段：
        

        message DocGzMeta {
        string path = 1; // HDFS路径，实际离线存储的时候会做一定的压缩，例如不存储公共前缀
        uint64 offset = 2; // 数据起始偏移量
        uint32 length = 3; // 数据长度
        }
        
*   *DocIdList* ：
    一个分词可能会出现多个文档中，每个文档有多行原始数据组成。   
    每个关联数据需要 docId、rawIndex 两个信息来描述。   

*   *InvertedIndex* ：倒排索引结构，搜索引擎中的核心数据结构，一般包含1000个 *Token* 及其索引信息。  
        
        map<string/*分词*/, DocIdList> index = 1;
        
*   *InvertedIndexGz* ： *InvertedIndex* 数据结构序列化之后的数据，然后使用gz压缩。  
    
*   *InvertedIndexGzHDFSFile* ：在hdfs上存储的倒排索引结构文件，该文件是由一组 *InvertedIndexGz* 直接拼接在一起的文件。     
    
*   *InvertedIndexGzMeta*  ： *InvertedIndexGz* 文件的元数据信息。包含下列几个字段：
        
        message InvertedIndexGzMeta {
        uint64 offset = 1; // 某一个 *InvertedIndexGzMeta* 所在 hdfs 文件中的起始地址偏移量
        uint32 length = 2; // *InvertedIndexGzMeta* 的所占数据长度
        string path = 3; // 一般存放的是文件名的关键字段信息，完整路径可以根据时间、索引表名、hashid等信息推断出来
        //    uint32 hashid = 4; // 可以通过 *Token* 进行hash运算计算出来
        }
        
*   *InvertedIndexGzMetaHDFSFile* ：在hdfs上存储的倒排索引结构meta文件，该文件是由一组 *InvertedIndexGzMeta* 直接拼接在一起的文件。
    一般可以将 *InvertedIndexGzMeta* 从 *InvertedIndexGzMetaHDFSFile* 构建到nosql数据库中。
