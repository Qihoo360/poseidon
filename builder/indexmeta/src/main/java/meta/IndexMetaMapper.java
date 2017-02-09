package meta;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

import proto.PoseidonIf;

/**
 * Created by liwei on 9/22/16.
 */

public class IndexMetaMapper extends Mapper<LongWritable, Text, Text, Text> {
    private String field_;
    private String fileName_;
    private String metaService_;
    private String bussiness_;
    private String metaUrl_;
    private String logDay_;
    private String shortDay_;
    private StringBuilder lines_ = new StringBuilder(1024);
    private int count_ = 0;

    @Override
    protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
            throws java.io.IOException, java.lang.InterruptedException {
        super.setup(context);
        System.err.println(context.getInputSplit().toString());
        FileSplit split = (FileSplit) context.getInputSplit();
        String indexNode = split.getPath().getParent().getName();
        int pos = indexNode.indexOf("gzmeta");
        if (pos == -1) {
            System.err.println(split.toString() + " format error");
            System.exit(-1);
        }
        field_ = indexNode.substring(0, pos);
        fileName_ = split.getPath().getName();
        metaService_ = context.getConfiguration().get("meta_service");
        bussiness_ = context.getConfiguration().get("log_name");
        metaUrl_ = "http://" + metaService_ + "/service/meta/" + bussiness_ + "/index/set";
        logDay_ = context.getConfiguration().get("log_day");

        shortDay_ = logDay_.replace("-", "").substring(2);

        System.err.println("get field: " + field_);
        System.err.println("get fileName: " + fileName_);
        System.err.println("get url: " + metaUrl_);
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
        proto.PoseidonIf.InvertedIndexGzMeta.Builder gzmetaBuilder = proto.PoseidonIf.InvertedIndexGzMeta.newBuilder();

        gzmetaBuilder.setPath(fileName_);
        gzmetaBuilder.setOffset(Integer.parseInt(meta[2]));
        gzmetaBuilder.setLength(Integer.parseInt(meta[3]));
        proto.PoseidonIf.InvertedIndexGzMeta gzmeta = gzmetaBuilder.build();
        String base64 = new String(Base64.encodeBase64(gzmeta.toByteArray()));
        String indexid = meta[0];
        String line = field_ + shortDay_ + indexid + "\t" + base64;
        context.write(new Text(field_), new Text(line));

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

        int maxRetryCnt = 10;
        int retryCnt = 0;
        String toPostStr = lines_.toString();
        while (true) {
            MetaSetter metaSetter = new MetaSetter(metaUrl_);
            String result = metaSetter.Post(toPostStr);
            if (result == null || !result.contains("OK")) {
                System.err.println("#" + retryCnt + " meta set error: " + toPostStr + "  " + result);
                ++retryCnt;
                if(retryCnt >= maxRetryCnt) {
                    System.err.println("Already try max times " + retryCnt + " for " + toPostStr);
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {}
            } else {
                break;
            }
        }

        lines_.delete(0, lines_.length());
        count_ = 0;
    }

}
