package InvertedIndex;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;

import InvertedIndex.plugin.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Task.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import util.MurmurHash3;

public class InvertedIndexGenerateMapper extends Mapper<LongWritable, Text, Text, Text> {

    private int total_size_ = 0; // file size, offset
    private int total_line_count_ = 0; //
    private long begin_docid_ = -1;
    private String log_name_ = null;
    private int total_line_per_doc_ = 0;
    private int is_middle_ = 0;
    private int log_dir_level_ = 0;
    private int process_line_count_ = 0;
    private LogParser logparser = null;

    static {
        System.err.println("InvertedIndexGenerateMapper-Encoding: " + Charset.defaultCharset().displayName());
    }

    @Override
    protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
            throws java.io.IOException, java.lang.InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        total_line_per_doc_ = conf.getInt("total_line_per_doc", 128);
        log_dir_level_ = conf.getInt("log_dir_level", 1);
        FileSplit split = (FileSplit) context.getInputSplit();
        String filename = GetFileName(split);
        GetBeginDocId(filename, context);

        LogParserFactory logParserFactory = new LogParserFactory();
        logparser = logParserFactory.Create(context.getConfiguration());
    }

    private String GetFileName(FileSplit split) {
        String filename = new String();
        String fname = split.getPath().getName();
        String hour = split.getPath().getParent().getName();
        if (fname.startsWith("part")) {
            is_middle_ = 1;
        }

        System.err.println("GetFileName: " + fname + "  hour:" + hour);

        //String day = split.getPath().getParent().getParent().getName();
        //String[] paths = fname.split("-");
        //filename = hour + paths[1].substring(0, 5);
        if (log_dir_level_ > 1) {
            filename = hour + fname; //paths[0].substring(0, paths[0].length()-5) + paths[3]+paths[4].substring(0, 1);
        } else {
            filename = fname;
        }
        //filename = hour + "/" + fname;
        return filename;
    }

    private void GetBeginDocId(String filename, org.apache.hadoop.mapreduce.Mapper.Context context) {
        System.err.println("GetBeginDocId: " + filename);
        BufferedReader in = null;
        try {
            Path[] cacheFiles = null;
            try {
                cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            for (int i = 0; i < cacheFiles.length; ++i) {
                String path = cacheFiles[i].toString();
                System.err.println(path);
                if (in == null && path.indexOf("fname_begin_docid.txt") != -1) {
                    System.err.println("found fname_begin_docid.txt:" + path);
                    String prefix = "file:";
                    if (path.startsWith(prefix)) {
                        path = path.substring(prefix.length());
                    }
                    in = new BufferedReader(new FileReader(path));
                }
            }

            try {
                if (in != null) {
                    String s = null;
                    while ((s = in.readLine()) != null) {
                        String[] fname_docid_parts = s.split("[ \t]");//old is \t
                        if (fname_docid_parts[0].compareTo(filename) == 0) {
                            begin_docid_ = Long.parseLong(fname_docid_parts[1]);
                            System.err.println("get begin id: " + begin_docid_);
                            break;
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        if (begin_docid_ == -1) {
            System.err.println("def begin id: -1");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        if (process_line_count_ % 100 == 0) {
            context.progress();
        }
        ++process_line_count_;
        if (is_middle_ == 1) {
            if (value.toString().isEmpty()) {
                return;
            }
            String val = value.toString();
            int token_pos = val.indexOf("\t");
            //String[] arr = value.toString().split("\t", 2);
            if (token_pos <= 0 || token_pos > 256) {
                return;
            }
            String token = val.substring(0, token_pos);
            byte[] token_buf = token.getBytes();
            int token_hash = MurmurHash3.murmurhash3_x86_32(token_buf, 0, token_buf.length, 0);
            if (token_hash < 0) {
                token_hash = 0 - token_hash;
            }
            int partition_hash = token_hash / 200;
            String new_key = String.format("%08d", partition_hash);
            context.write(new Text(new_key), new Text(value.toString()));
            return;
        }
        if (begin_docid_ == -1) {
            cleanup(context);
            context.setStatus("0");
            //context.getCounter(Counter.MAP_READ_WALLCLOCK).setValue(0);
            System.exit(0);
        }

        if (value != null) {
            /*
            //if (line_ != "") {
			//	line_  += value.toString();
			//} else {
				line_ = value.toString();
			}
			if (line_.charAt(line_.length() -1) == '\r'){
				return;
			}
			*/
            logparser.ParseLine(value.toString(), begin_docid_ + total_line_count_ / total_line_per_doc_,
                    total_line_count_ % total_line_per_doc_, context);
            total_line_count_++;
            //line_ = "";
        }
    }
}
