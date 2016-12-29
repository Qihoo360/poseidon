#!/bin/bash
# Source global definitions
#if [ -f /etc/bashrc ]; then
#. /etc/bashrc
#fi

which java
if [ $? -ne 0 ];
then
   echo java sdk not installed
   exit
fi

JAVA=java
#export JAVA_HOME=/usr/bin/hadoop/software/java/
#export CLASSPATH=/usr/bin/hadoop/software/java/lib
#export HADOOP_HOME=/usr/bin/hadoop/software/hadoop/

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
index_process_base_path=`dirname $dir`
echo index_process_base_path $index_process_base_path

for path in `find ${index_process_base_path}/lib -name "*.jar"`
do
    export HADOOP_CLASSPATH=$path:$HADOOP_CLASSPATH
done

export PATH=$PATH:$JAVA_HOME/bin
export LANG="en_US.UTF-8"

day=`date -d "0 hours ago" +"%Y-%m-%d"`
day=$1

bussiness=test
hdp_src_base=/home/poseidon/src/
hdp_index_base=$hdp_src_base

hdp_src=${hdp_src_base}/${bussiness}/${day}
hdp_doc_src=${hdp_src_base}/${bussiness}/docid/${day}/
hdp_dst=$hdp_index_base/$bussiness/index/$day
hdp_dst2=$hdp_index_base/$bussiness/index/$day/meta
hdp_doc_dst=$hdp_index_base/$bussiness/firstdocid/$day/

echo bussiness $bussiness
echo hdp_src $hdp_src

mkdir -p ${index_process_base_path}/index/logs
exec 2>> ${index_process_base_path}/index/logs/${day}.log
set -x 

cd ${index_process_base_path}
echo index_process_base_path  $index_process_base_path


rm -rf $hdp_doc_dst
cmd="$JAVA -classpath $HADOOP_CLASSPATH meta.DocMetaConfigured $hdp_doc_src  $hdp_doc_dst $day ${index_process_base_path}/etc/test.json "
echo $cmd
$cmd

if [ -e ${hdp_doc_dst}/_SUCCESS ]; then
    echo dco meta setter map reduce "${bussiness} ${day} success"
    rm -rf  ${hdp_index_base}/${bussiness}/conf/${day}/fname_begin_docid.txt
    mkdir -p ${hdp_index_base}/${bussiness}/conf/${day}/
    mv $hdp_doc_dst/part-r-00000 ${hdp_index_base}/${bussiness}/conf/${day}/fname_begin_docid.txt
else
    echo doc meta setter map reduce "${bussiness} ${day} failed"
    exit -1;
fi


rm -rf $hdp_dst
cmd="$JAVA -classpath $HADOOP_CLASSPATH InvertedIndex.InvertedIndexGenerate $hdp_src $hdp_dst $day ${index_process_base_path}/etc/test.json "
echo $cmd
$cmd
if [ -e ${hdp_dst}/_SUCCESS ]; then
    echo map reduce "${bussiness} ${day} success"
else
    echo map reduce "${bussiness} ${day} failed"
    exit -1;
fi


rm -rf $hdp_dst2
cmd="$JAVA -classpath $HADOOP_CLASSPATH  meta.IndexMetaConfigured $hdp_dst/*gzmeta  $hdp_dst2 $day ${index_process_base_path}/etc/test.json "
echo $cmd
$cmd

if [ -e ${hdp_dst}/_SUCCESS ]; then
    echo index meta setter map reduce "${bussiness} ${day} success"
else
    echo index meta setter map reduce "${bussiness} ${day} failed"
    exit -1;
fi


