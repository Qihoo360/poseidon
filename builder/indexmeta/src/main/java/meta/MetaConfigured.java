package meta;

import InvertedIndex.plugin.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.URI;

/**
 * Created by liwei on 9/27/16.
 */
public abstract class MetaConfigured extends Configured implements Tool {


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
        String fs_default_name="";
        if (mock == "true") {
            fs_default_name = System.getProperty("user.dir");
        } else {
            String name_node = conf.get("name_node");
            //fs_default_name = "hdfs://" + name_node + "/";
        }

        Job job = GetMapReduceJob(conf);
        String hdpfs_index_base_path = conf.get("hdpfs_index_base_path");
        String bussiness = conf.get("log_name");

        String[] libs_files = conf.get("libs").split(",");
        for (int i = 0; i < libs_files.length; i++) {
            if (libs_files[i].isEmpty()) continue;
            System.err.println("libs-" + i + ": " + libs_files[i]);
            String file = libs_files[i];
            String hdfsFile = fs_default_name + hdpfs_index_base_path + "/" + bussiness + "/lib/" + day + "/" + file;

            InvertedIndex.HDFSFileWriter.UploadFile("lib/" + file, hdfsFile);
            DistributedCache.addFileToClassPath(new Path(hdfsFile), job.getConfiguration());
        }

        String[] etcs_files = conf.get("etcs").split(",");
        for (int i = 0; i < etcs_files.length; i++) {
            if (etcs_files[i].isEmpty()) continue;
            System.err.println("etcs-" + i + ": " + etcs_files[i]);
            String file = etcs_files[i];
            String hdfsFile = fs_default_name + hdpfs_index_base_path + "/" + bussiness + "/conf/" + day + "/" + file;

            InvertedIndex.HDFSFileWriter.UploadFile("etc/" + file, hdfsFile);
            DistributedCache.addCacheFile(new URI(hdfsFile), job.getConfiguration());
        }

        String[] paths = arg0[0].split(",");
        for (int i = 0; i < paths.length; i++) {
            if (paths[i].isEmpty()) continue;
            String hdfsFile = fs_default_name + paths[i];
            System.err.println("add input:" + hdfsFile);
            FileInputFormat.addInputPath(job, new Path(hdfsFile));
        }

        FileOutputFormat.setOutputPath(job, new Path(fs_default_name + arg0[1]));
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

            String hdpfs_index_base_path = common.getString("hdpfs_index_base_path");

            if (hdpfs_index_base_path.isEmpty()) {
                System.err.println("hdpfs_index_base_path  is empty");
                System.exit(-1);
            }

            Boolean mock = common.getBoolean("local_mock");
            if (mock) {
                conf.set("fs.default.name", "file:///");
                conf.set("mapred.job.tracker", "local");
                conf.set("local_mock", "true");
            } else {
                //conf.set("fs.default.name", "hdfs:///");
            }

            conf.set("log_name", bussiness.intern());
            conf.set("name_node", name_node);
            conf.set("hdpfs_index_base_path", hdpfs_index_base_path);

            String hdfs_path = inverted_index.getString("hdfs_path");
            conf.set("hdfs_path", hdfs_path);

            String meta_service = common.getString("meta_service");
            conf.set("meta_service", meta_service);
            conf.set("json_conf", inverted_index.toString());
            conf.set("etcs", inverted_index.getString("etcs"));
            conf.set("libs", inverted_index.getString("libs"));

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

    public abstract Job GetMapReduceJob(Configuration conf);
}


