# hdfsReader

------

功能：根据给定offset和length读取给定文件path

# read-hdfs

* 调用方式：
    http://ip:port/read-hdfs?path=...&offset=0&length=1123

* 参数：
    * path：hdfs文件路径,支持归档文件读取
    * offset：从offset开始读取
    * length：读取 length 个字节

* example:
    * http://ip:port/read-hdfs?path=/home/poseidon/gpdata/2016-08-25/parr-00930.gz&offset=62628504&length=30929
    * http://ip:port/read-hdfs?path=har:///home/archive/poseidon/2016-06-04.har/2016-06-04/00/002000_2016-06-04-00-00.gz&offset=319383859&length=13110

# read-localfs

* 调用方式：
    * http://ip:port/read-localfs?path=...&offset=0&length=1123

* 参数：
    * path：本地文件路径
    * offset：从offset开始读取（从0开始）
    * length：读取 length 个字节

* example:
    * http://ip:port/read-localfs?path=/tmp/test.gz&offset=62628504&length=30929
