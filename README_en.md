# Poseidon

Poseidon is a log searching engine system that can quickly search and retrieve specific strings in hundreds of petabytes and hundreds of trillion lines of log data.
In the past, if we try to find some information in such large set of data, we need to write a Map/Reduce task program and run it in Hadoop platform.
This will cost several hours, maybe more, that greatly restricting the working efficiency.

The Poseidon system can solve this problem, it builds the inverted index directly on HDFS and does not change the storing mode of the original log data which is still stored on HDFS.
That means the Poseidon system does not need to store the original log data.
We can use Poseidon system to do searching and use Hadoop system to run Map/Reduce task in the same time and on the same data set.

That is very different with ElasticSearch which stores the index data and original data in its own system and stores the original data on HDFS if we also need to run Map/Reduce task.
And more importantly, ElasticSearch cannot hold such large scale of data set.

