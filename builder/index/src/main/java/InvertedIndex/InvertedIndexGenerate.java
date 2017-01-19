package InvertedIndex;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import InvertedIndex.plugin.LogParserFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONException;
import org.json.JSONObject;
import InvertedIndex.plugin.Util;

public class InvertedIndexGenerate extends Configured implements Tool {

    public static void main(String[] args)
            throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        int ret = 0;
        try {
            ret = ToolRunner.run(new InvertedIndexGenerate(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(ret);
    }

    @Override
    public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = new Configuration();

        // 这些参数固定
        conf.set("mapred.job.priority", "VERY_HIGH");
        conf.set("mapred.ignore.badcompress", "true");
        conf.setLong("mapred.linerecordreader.maxlength", 10 * 1024 * 1024);
        conf.setLong("mapred.max.split.size", 512 * 1024 * 1024);
        conf.setBoolean("mapreduce.user.classpath.first", true);
        conf.setBoolean("mapred.compress.map.output", true);

        conf.setBoolean("mapred.success.file.status", true);
        conf.setBoolean("mapred.map.tasks.speculative.execution", true);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        conf.setInt("mapred.max.map.failures.percent", 1);

        conf.set("mapred.child.env", "LANG=en_US.UTF-8,LC_ALL=en_US.UTF-8");
        conf.setInt("mapred.job.max.map.running", 1500);

        conf.set("mapred.child.env", "LANG=en_US.UTF-8,LC_ALL=en_US.UTF-8");
        conf.set("mapred.task.timeout", "36000000");  //ms
        //conf.set("dfs.socket.timeout", "3600000");
        //conf.set("dfs.datanode.socket.write.timeout", "3600000");

        String index_config = new String("index.json");
        String day = arg0[2];
        conf.set("log_day", day);

        if (arg0.length > 3) {
            index_config = arg0[3];
        }

        InitJsonParams(index_config, conf);
        System.err.println("index_config: " + index_config);
        String mock = conf.get("local_mock", "false");
        String fs_default_name = "";
        if (mock == "true") {
            //fs_default_name = System.getProperty("user.dir");
        } else {
            String name_node = conf.get("name_node");
            //fs_default_name = "hdfs://" + name_node;
        }
        String bussiness = conf.get("log_name");
        String hdpfs_index_base_path = conf.get("hdpfs_index_base_path");
        //转换后的doc日志路径
        String hdfs_path = conf.get("hdfs_path");
        String urlBase = fs_default_name + hdpfs_index_base_path + "/";
        String docidPath = fs_default_name + hdfs_path + "/docid/" + day + "/";

        String fNameBeginDocid = fs_default_name + hdpfs_index_base_path + "/" + bussiness + "/conf/" + day + "/fname_begin_docid.txt";
        System.err.println(fNameBeginDocid);
        //HDFSFileWriter.CreateFirstDocidList(docidPath, fNameBeginDocid);

        Job job = new Job(conf, InvertedIndexGenerate.class.getSimpleName());
        DistributedCache.addCacheFile(new URI(fNameBeginDocid), job.getConfiguration());

        String[] libs_files = conf.get("libs").split(",");
        for (int i = 0; i < libs_files.length; i++) {
            if (libs_files[i].isEmpty()) continue;
            System.err.println("libs-" + i + ": " + libs_files[i]);
            String file = libs_files[i];
            String hdfsFile = fs_default_name + hdpfs_index_base_path + "/" + bussiness + "/lib/" + day + "/" + file;

            HDFSFileWriter.UploadFile("lib/" + file, hdfsFile);
            DistributedCache.addFileToClassPath(new Path(hdfsFile), job.getConfiguration());
        }

        String[] etcs_files = conf.get("etcs").split(",");
        for (int i = 0; i < etcs_files.length; i++) {
            if (etcs_files[i].isEmpty()) continue;
            System.err.println("etcs-" + i + ": " + etcs_files[i]);
            String file = etcs_files[i];
            String hdfsFile = fs_default_name + hdpfs_index_base_path + "/" + bussiness + "/conf/" + day + "/" + file;

            HDFSFileWriter.UploadFile("etc/" + file, hdfsFile);
            DistributedCache.addCacheFile(new URI(hdfsFile), job.getConfiguration());
        }

        String[] filter_files = conf.get("filterfiles").split(",");
        for (int i = 0; i < filter_files.length; i++) {
            if (filter_files[i].isEmpty()) continue;
            System.err.println("filter_config-" + i + ": " + filter_files[i]);
            String file = filter_files[i];
            String hdfsFile = fs_default_name + hdpfs_index_base_path + "/" + bussiness + "/conf/" + day + "/" + file;

            HDFSFileWriter.UploadFile("conf/" + file, hdfsFile);
            DistributedCache.addCacheFile(new URI(hdfsFile), job.getConfiguration());
        }

        job.setJarByClass(InvertedIndexGenerate.class);
        job.setInputFormatClass(NTextInputFormat.class);
        //MultipleOutputs.addNamedOutput(job, "gzmeta",TextOutputFormat.class, Text.class, Text.class);
        job.setMapperClass(InvertedIndexGenerateMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(InvertedIndexGenerateReducer.class);
        job.setOutputFormatClass(IndexGzMetaOutputFormat.class);
        job.setCombinerClass(InvertedIndexGenerateCombiner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        String[] paths = arg0[0].split(",");
        for (int i = 0; i < paths.length; i++) {
            if (paths[i].isEmpty()) continue;
            FileInputFormat.addInputPath(job, new Path(paths[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
        job.waitForCompletion(true);
        return 0;
    }

    private void InitParams(JSONObject jsConf, Configuration conf) {
        try {

            JSONObject common = jsConf.getJSONObject("common");
            JSONObject inverted_index = jsConf.getJSONObject("inverted_index");

            if (common == null) {
                System.err.println("conf can not find common");
                System.exit(-1);
            }

            String name_node = common.getString("name_node");
            if (name_node.isEmpty()) {
                System.err.println("name node is empty");
                System.exit(-1);
            }

            String bussiness = common.getString("bussiness");
            if (bussiness.isEmpty()) {
                System.err.println("bussiness name is empty");
                System.exit(-1);
            }

            Boolean mock = common.getBoolean("local_mock");
            if (mock) {
                conf.set("fs.default.name", "file:///");
                conf.set("mapred.job.tracker", "local");
                //conf.set("mapreduce.framework.name", "local");
                conf.set("local_mock", "true");
            } else {
                //conf.set("fs.default.name", "hdfs:///");
            }

            conf.set("log_name", bussiness.intern());

            String hdpfs_index_base_path = common.getString("hdpfs_index_base_path");

            if (hdpfs_index_base_path.isEmpty()) {
                System.err.println("hdpfs_index_base_path  is empty");
                System.exit(-1);
            }
            conf.set("name_node", name_node);
            conf.set("hdpfs_index_base_path", hdpfs_index_base_path);

            int line_per_doc = common.getInt("line_per_doc");
            if (line_per_doc > 0) {
                conf.setInt("line_per_doc", line_per_doc);
            } else {
                conf.setInt("total_line_per_doc", 128);
            }

            int indexgzmeta_section = common.getInt("indexgzmeta_section");
            if (indexgzmeta_section > 0) {
                conf.setInt("hash_num_per_indexgzmeta", indexgzmeta_section);
            } else {
                conf.setInt("hash_num_per_indexgzmeta", 200);
            }

            int indexgz_file_num_per_field = common.getInt("indexgz_file_num_per_field");
            if (indexgz_file_num_per_field > 0) {
                conf.setInt("mapred.reduce.tasks", indexgz_file_num_per_field);
                conf.setInt("indexgz_file_num_per_field", indexgz_file_num_per_field);
            } else {
                conf.setInt("mapred.reduce.tasks", 1000);
                conf.setInt("indexgz_file_num_per_field", indexgz_file_num_per_field);
            }

            String hadoop_user = common.getString("hadoop_user");
            if (hadoop_user.isEmpty()) {
                //do noting
            } else {
                conf.set("mapred.fairscheduler.pool", hadoop_user.intern());
            }

            int save_line_count_per_map = inverted_index.getInt("save_line_count_per_map");
            if (save_line_count_per_map > 0) {
                conf.setInt("save_line_count_per_map", indexgz_file_num_per_field);
            } else {
                conf.setInt("save_line_count_per_map", 100000);
            }

            String hdfs_path = inverted_index.getString("hdfs_path");
            conf.set("hdfs_path", hdfs_path);
            conf.set("etcs", inverted_index.getString("etcs"));
            conf.set("libs", inverted_index.getString("libs"));


            //TODO
            //log_dir_level = 1  day
            //log_dir_level = 2  hour
            //即将废弃
            int log_dir_level = inverted_index.getInt("log_dir_level");
            if (log_dir_level == 0) {
                conf.setInt("log_dir_level", 1);
            } else {
                conf.setInt("log_dir_level", log_dir_level);
            }

            conf.set("cache_dir", hdpfs_index_base_path + "/" + bussiness + "/cache/" + conf.get("log_day"));

            conf.set("json_conf", inverted_index.toString());
            System.err.println("get inverted_index string:");
            System.err.println(inverted_index.toString());

            long cache_size = inverted_index.getLong("cache_size");
            if (cache_size == 0) {
                conf.setLong("cache_size", 16777216L);
            } else {
                conf.setLong("cache_size", cache_size);
            }

            int debug = inverted_index.getInt("debug");
            if (debug == 0) {
                conf.setInt("_debug_", 0);
            } else {
                conf.setInt("_debug_", debug);
            }
            System.err.println("DEBUG: " + debug);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }


    private void InitJsonParams(String conf_file, Configuration conf) {
        String json_conf = Util.ReadFile(conf_file);
        if (!json_conf.isEmpty()) {
            conf.set("json_conf", json_conf);
            try {
                System.err.println(json_conf);
                JSONObject json = new JSONObject(json_conf);
                InitParams(json, conf);
                LogParserFactory logParserFactory = new LogParserFactory();
                logParserFactory.Init(conf);

            } catch (JSONException e) {
                e.printStackTrace();
            }
        } else {
            System.err.println("json conf is empty");
            System.exit(-1);
        }
        return;
    }

    @Override
    public void setConf(Configuration conf) {
        // TODO Auto-generated method stub
    }

    @Override
    public Configuration getConf() {
        // TODO Auto-generated method stub
        return null;
    }
}
