package meta;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by liwei on 9/26/16.
 */


public class DocMetaConfigured extends MetaConfigured {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        int ret = 0;
        try {
            ret = ToolRunner.run(new DocMetaConfigured(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(ret);
    }

    @Override
    public Job GetMapReduceJob(Configuration conf) {
        try {
            conf.setInt("mapred.reduce.tasks", 1);
            Job job = new Job(conf, DocMetaConfigured.class.getSimpleName());
            System.err.println("get doc meta job ");
            job.setJarByClass(DocMetaConfigured.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(DocMetaMapper.class);
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
