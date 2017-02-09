package meta;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

import proto.PoseidonIf.DocGzMeta;

/**
 * Created by liwei on 9/27/16.
 */
public class DocMetaMapper extends Mapper<LongWritable, Text, Text, Text> {

    private String metaService_;
    private String bussiness_;
    private String metaUrl_;
    private String logDay_;
    private String shortDay_;
    private StringBuilder lines_ = new StringBuilder(1024);
    private int count_ = 0;
    private boolean firstkey_ = true;
    private String filename_;

    @Override
    protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
            throws java.io.IOException, java.lang.InterruptedException {
        super.setup(context);
        System.err.println(context.getInputSplit().toString());
        metaService_ = context.getConfiguration().get("meta_service");
        bussiness_ = context.getConfiguration().get("log_name");
        metaUrl_ = "http://" + metaService_ + "/service/meta/" + bussiness_ + "/doc/set";
        logDay_ = context.getConfiguration().get("log_day");
        shortDay_ = logDay_.replace("-", "").substring(2);
        FileSplit split = (FileSplit) context.getInputSplit();
        filename_ = split.getPath().getName();
        System.err.println("get shortDay: " + shortDay_);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        System.err.println("cleanup");
        set();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {


        //System.err.println(value.toString());
        String[] meta = value.toString().split("\t");
        if (meta.length < 2) {
            return;
        }
        if (firstkey_) {
            firstkey_ = false;
            context.write(new Text(filename_), new Text(meta[0]));
        }
        //System.err.println(meta[1]);
        //byte[] rawPb = Base64.decodeBase64(meta[1]);
        //proto.PoseidonIf.DocGzMeta gzmeta =  proto.PoseidonIf.DocGzMeta.parseFrom(rawPb);
        //System.err.println(gzmeta.toString());

        String line = shortDay_ + meta[0] + "\t" + meta[1];
        lines_.append(line).append('\n');
        count_ += 1;
        if (count_ >= 50) {
            set();
        }

    }

    private void set() {
        if(lines_.length() <= 0) {
            return;
        }
        int maxRetryCnt = 8;
        int retryCnt = 0;
        String toPostStr = lines_.toString();
        while (true) {
            MetaSetter metaSetter = new MetaSetter(metaUrl_);
            String result = metaSetter.Post(toPostStr);
            if (result == null || !result.contains("OK")) {
                System.err.println("#" + retryCnt +" meta set error: " + toPostStr + "  " + result);
                ++retryCnt;
                if(retryCnt >= maxRetryCnt) {
                    System.err.println("Already try max times " + retryCnt + " for " + toPostStr);
                    break;
                }
                try {
                    Thread.sleep(1001);
                } catch (InterruptedException e) {}
            } else {
                break;
            }
        }
        lines_.delete(0, lines_.length());
        count_ = 0;
    }
}
