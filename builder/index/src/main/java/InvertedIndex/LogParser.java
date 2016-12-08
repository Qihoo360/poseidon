package InvertedIndex;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.MurmurHash3;


public abstract class LogParser {
    public abstract void ParseLine(String line, long docid, int line_offset,
                                   Mapper<LongWritable, Text, Text, Text>.Context context);

    static {
        System.err.println("LogParser-Encoding: " + Charset.defaultCharset().displayName());
    }

    protected void Write(String token, String type, long docid, int line_offset,
                         Mapper<LongWritable, Text, Text, Text>.Context context) {
        if (token.isEmpty() || token.compareTo("null") == 0) {
            return;
        }
        byte[] token_buf = token.getBytes();
        int token_hash = MurmurHash3.murmurhash3_x86_32(token_buf, 0, token_buf.length, 0);

        if (token_hash < 0) {
            token_hash = 0 - token_hash;
        }
        int partition_hash = token_hash / 200;
        String new_key = String.format("%08d", partition_hash);
        //String line = token + "\t" + type +"\t" +  docid +"\t" + line_offset;
        //StringBuffer ss = new StringBuffer();
        //ss.append(token).append("\t").append(type).append("\t").append("1").append("\t").append(docid).append(",").append(line_offset);
        String ss = token + "\t" + type + "\t" + docid + "," + line_offset + "\t" + 1;
        try {
            //context.write(new Text(new_key), new Text(line));
            context.write(new Text(new_key), new Text(ss));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            // e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            // e.printStackTrace();
        } catch (Exception e) {
            // 可能会有异常java.io.IOException: Map Task output too many bytes! which shouldn't be more than 21474836480
            // 全部输出的话会产生ERROR org.apache.hadoop.mapred.Child: Error in syncLogs: java.io.IOException: Tasklog stderr size 1096750402 exceeded limit 1073741824
            //e.printStackTrace();
        }
        //System.err.println(ss);
    }
}
