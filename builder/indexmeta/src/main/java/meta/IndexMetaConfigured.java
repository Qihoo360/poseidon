package meta;

import meta.MetaConfigured;
import meta.IndexMetaMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URISyntaxException;


/**
 * Created by liwei on 9/22/16.
 */
public class IndexMetaConfigured extends MetaConfigured {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        int ret = 0;
        try {
            ret = ToolRunner.run(new IndexMetaConfigured(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(ret);
    }

    @Override
    public Job GetMapReduceJob(Configuration conf) {
        try {
            Job job = new Job(conf, IndexMetaConfigured.class.getSimpleName());
            System.err.println("get index meta job ");
            job.setJarByClass(IndexMetaConfigured.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(IndexMetaMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(BytesWritable.class);
            return job;
        } catch (Exception e) {

        }
        return null;
    }
}
