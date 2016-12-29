#!/bin/bash
# Source global definitions
if [ -f /etc/bashrc ]; then
. /etc/bashrc
fi
#export JAVA_HOME=/usr/bin/hadoop/software/java/
#export CLASSPATH=/usr/bin/hadoop/software/java/lib
#export HADOOP_HOME=/usr/bin/hadoop/software/hadoop/
for path in `find $HADOOP_HOME -name "*.jar"`
do
    export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$path
done

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
index_process_base_path=`dirname $dir`
echo index_process_base_path $index_process_base_path

for path in `find ${index_process_base_path}/lib -name "*.jar"`
do
    export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$path
done

export HBASE_CLASSPATH=$HBASE_CLASSPATH:$HADOOP_CLASSPATH:$HADOOP_HOME/conf
export HADOOP_CLASSPATH=$HBASE_CLASSPATH
export CLASSPATH=$HBASE_CLASSPATH:$CLASSPATH
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH
export LANG="en_US.UTF-8"

HADOOP=/usr/local/hadoop/bin/hadoop
day=`date -d "0 hours ago" +"%Y-%m-%d"`
day=$1

bussiness=test
hdp_src_base=/home/poseidon/src/
hdp_index_base=$hdp_src_base

hdp_src=${hdp_src_base}/${bussiness}/${day}/*.gz
hdp_doc_src=${hdp_src_base}/${bussiness}/docid/${day}/
hdp_dst=$hdp_index_base/$bussiness/index/$day
hdp_dst2=$hdp_index_base/$bussiness/index/$day/meta
hdp_doc_dst=$hdp_index_base/$bussiness/firstdocid/$day/

echo bussiness $bussiness
echo hdp_src $hdp_src

mkdir ${index_process_base_path}/index/logs
exec 2>> ${index_process_base_path}/index/logs/${day}.log
set -x 

cd ${index_process_base_path}
current_dir=`pwd`
echo current_dir  $current_dir

$HADOOP  fs -rmr $hdp_doc_dst
cmd="$HADOOP jar ${current_dir}/lib/docmeta-1.0-SNAPSHOT.jar meta.DocMetaConfigured $hdp_doc_src  $hdp_doc_dst $day ${current_dir}/etc/test.json "
echo $cmd
$cmd

echo $HADOOP fs -test -e ${hdp_doc_dst}/_SUCCESS
$HADOOP fs -test -e ${hdp_doc_dst}/_SUCCESS
if [ $? == 0 ]; then
    echo dco meta setter map reduce "${bussiness} ${day} success"
    $HADOOP fs -rmr ${hdp_index_base}/${bussiness}/conf/${day}/fname_begin_docid.txt
    $HADOOP fs -mv $hdp_doc_dst/part-r-00000 ${hdp_index_base}/${bussiness}/conf/${day}/fname_begin_docid.txt
else
    echo doc meta setter map reduce "${bussiness} ${day} failed"
    exit -1;
fi


$HADOOP  fs -rmr $hdp_dst
cmd="$HADOOP jar ${current_dir}/lib/index-1.0-SNAPSHOT.jar InvertedIndex.InvertedIndexGenerate $hdp_src,$hdp_last_middle $hdp_dst $day ${current_dir}/etc/test.json "
echo $cmd
$cmd
#
echo $HADOOP fs -test -e ${hdp_dst}/_SUCCESS
$HADOOP fs -test -e ${hdp_dst}/_SUCCESS
if [ $? == 0 ]; then
    echo map reduce "${bussiness} ${day} success"
else
    echo map reduce "${bussiness} ${day} failed"
    exit -1;
fi



$HADOOP  fs -rmr $hdp_dst2
cmd="$HADOOP jar ${current_dir}/lib/indexmeta-1.0-SNAPSHOT.jar meta.IndexMetaConfigured $hdp_dst/*gzmeta  $hdp_dst2 $day ${current_dir}/etc/test.json "
echo $cmd
$cmd

echo $HADOOP fs -test -e ${hdp_dst2}/_SUCCESS
$HADOOP fs -test -e ${hdp_dst2}/_SUCCESS
if [ $? == 0 ]; then
    echo meta setter map reduce "${bussiness} ${day} success"
else
    echo meta setter map reduce "${bussiness} ${day} failed"
    exit -1;
fi


