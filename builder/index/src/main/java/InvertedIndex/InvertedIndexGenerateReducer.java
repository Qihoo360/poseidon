package InvertedIndex;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.protobuf.InvalidProtocolBufferException;

import proto.PoseidonIf.*;
//import proto.PoseidonIf.InvertedIndex;
import InvertedIndex.plugin.Util;

public class InvertedIndexGenerateReducer extends Reducer<Text, Text, Text, BytesWritable> {

    // private IndexGzMetaOutputFormat multipleOutputs;
    private Map<String, Long> fields_file_size_; //filed
    private Set fields_ = null;

    private Text MIDDLE = new Text("middle");

    // 64m
    static long MaxDocidSpaceSize;
    static String CacheDir;

    static {
        System.err.println("InvertedIndexGenerateReducer-Encoding: " + Charset.defaultCharset().displayName());
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        fields_ = new HashSet();
        Configuration conf = context.getConfiguration();
        String fields = conf.get("fields");
        String[] fields_part = fields.split(",");
        fields_file_size_ = new HashMap();

        for (int i = 0; i < fields_part.length; i++) {
            if (!fields_part[i].isEmpty()) {
                // 把字段值变成常量
                (fields_part[i] + "index").intern();
                (fields_part[i] + "gzmeta").intern();
                fields_.add(fields_part[i].intern());
                // System.err.println(fields_part[i]);
                fields_file_size_.put(fields_part[i], 0L);
            }
        }

        MaxDocidSpaceSize = conf.getLong("cache_size", 67108864L);
        CacheDir = conf.get("cache_dir");

        System.err.println("DiskCacheSize: " + MaxDocidSpaceSize + ", DiskCacheDir: " + CacheDir);
    }

    /*
     * 优化：
     * 将values按key分组，每次只处理一组数据，输出
     *   内存使用上将是：小 -> 大 -> 小
     *
     *   优化大的这部分:
     *
     *   1整个values是一个key，代码将和现在一样
     *   2假设最差的可能性，单条数据会导致内存溢出
     * */
    @Override
    protected void reduce(Text key, Iterable<Text> values_ori, Reducer<Text, Text, Text, BytesWritable>.Context context)
            throws IOException, InterruptedException {

        String suffix = context.getTaskAttemptID().getTaskID().toString();
        suffix = suffix.substring(suffix.length() - 5, suffix.length());

        String key_str = key.toString();

        // values只能迭代一次，需要自己存起来再迭代
        ArrayList<Text> values = new ArrayList<Text>();
        for (Text v : values_ori) {
            values.add(new Text(v));
        }
        /*
         * 第一版的group是按"field_word"，输出的gzmeta中可能会有重复
		 * 需要有两个group：
		 *  group1：大group，用field，输出index,gzmeta
		 *  group2：小group，用field_word，输出middle
		 * */
        ArrayList<FieldSizeData> field_size = this.prepare(values);
        while (!field_size.isEmpty()) {
            FieldSizeData fsd = field_size.get(0);
            field_size.remove(0);

            if (fsd.size_ >= MaxDocidSpaceSize/*false*/) {
                // 走磁盘缓存
                System.err.println((new StringBuilder()).append("[DiskCache] size:")
                        .append(fsd.size_).append(", key:").append(key_str)
                        .append(", field:").append(fsd.field_).toString());

                runAsDiskCache(key_str, values, context, fsd.field_, suffix);
            } else {
                // 走内存
                runAsMemory(key_str, values, context, fsd.field_, suffix);
            }
        }

    }

    private synchronized void runAsDiskCache(String key, ArrayList<Text> values, Reducer<Text, Text, Text, BytesWritable>.Context context,
                                             String field, String suffix) throws IOException, InterruptedException {

        String cache_path = (new StringBuilder()).append(CacheDir).append("/").append(
                suffix).append("-").append(System.currentTimeMillis()).append(".cache").toString();
        Path cache_path_c = new Path(URI.create(cache_path));
        FileSystem cache_file = FileSystem.get(URI.create(cache_path), context.getConfiguration());
        if (cache_file.exists(cache_path_c)) {
            //cache_file.deleteUnusingTrash(cache_path_c, false);
            cache_file.delete(cache_path_c, false);
        }
        FSDataOutputStream out = cache_file.create(cache_path_c);
        if (out == null) {
            System.err.println("[DiskCache] failed: " + cache_path);
            return;
        }
        ArrayList<String> words = ReduceGroupData.getWords(values, field);
        int count = 0;

        while (!words.isEmpty()) {
            ++count;

            String word = words.get(0);
            words.remove(0);

            ReduceGroupData.MetaData md = ReduceGroupData.runWord(values, field, word);

            System.err.println((new StringBuilder()).append("[DiskCache] word:").append(word)
                    .append(", pv:").append(md.pv_).append(",base64:").append(Util.Base64EncoderStr(word, false)).toString());

            if ((count % 10) == 0) {
                context.progress();
            }

            StringBuffer ss = new StringBuffer();
            ss.append(word).append("\t").append(field);

            DocIdList docid_list = SortDocIdList(md.docid_list_build_, md.pv_);
            GetDocIdListStr(docid_list, ss);
            ss.append("\t").append(md.pv_).append("\n");
            // save middle
            context.write(MIDDLE, new BytesWritable(ss.toString().getBytes()));

            //index_build.getMutableIndex().put(word, docid_list);
            write_cache(out, word, docid_list.toByteArray());
        }
        out.close();

        InvertedIndex.Builder index_build = read_cache(cache_file, cache_path_c);
        output(key, context, field, suffix, index_build);

        //cache_file.deleteUnusingTrash(cache_path_c, true);
        cache_file.delete(cache_path_c, true);

    }

    private void runAsMemory(String key, ArrayList<Text> values, Reducer<Text, Text, Text, BytesWritable>.Context context,
                             String field, String suffix) throws IOException, InterruptedException {

        ReduceGroupData.Result result = ReduceGroupData.runGroup(values, field);
        InvertedIndex.Builder index_build = InvertedIndex.newBuilder();

        int count = 0;
        // output middle
        Iterator<String> itr = result.data_.keySet().iterator();
        while (itr.hasNext()) {
            if ((count++ % 10) == 0) {
                context.progress();
            }

            String word = itr.next();
            ReduceGroupData.MetaData md = result.data_.get(word);

            StringBuffer ss = new StringBuffer();
            ss.append(word).append("\t").append(field);

            DocIdList docid_list = SortDocIdList(md.docid_list_build_, md.pv_);
            GetDocIdListStr(docid_list, ss);
            ss.append("\t").append(md.pv_).append("\n");
            // save middle
            context.write(MIDDLE, new BytesWritable(ss.toString().getBytes()));

            index_build.getMutableIndex().put(word, docid_list);
        }
        output(key, context, field, suffix, index_build);
    }

    private void output(String key, Reducer<Text, Text, Text, BytesWritable>.Context context,
                        String field, String suffix, InvertedIndex.Builder index_build) throws IOException, InterruptedException {

        byte retArr[] = null;
        {

            ByteArrayOutputStream gzip_out_stream = null;
            {
                InvertedIndex index = index_build.build();
                index_build = null;

                int indexted_index_length = index.getSerializedSize();
                if (indexted_index_length < 10)
                    return;

                byte[] indexted_index = index.toByteArray();
                index = null;

                gzip_out_stream = new ByteArrayOutputStream();
                GZIPOutputStream gzip = new GZIPOutputStream(gzip_out_stream);
                gzip.write(indexted_index);
                gzip.finish();
                gzip.flush();
                gzip.close();
            }
            retArr = gzip_out_stream.toByteArray();
            gzip_out_stream = null;
        }

        // save index
        context.write(new Text(field + "index"), new BytesWritable(retArr));

        Number num = (Number) fields_file_size_.get(field);
        long current_length = num.longValue();
        StringBuffer ss = new StringBuffer();
        String val = ss.append(key).append("\t").append(suffix).append("\t").append(current_length
        ).append("\t").append(retArr.length).append("\n").toString();
        // save gzmeta
        context.write(new Text(field + "gzmeta"), new BytesWritable(val.getBytes()));

        fields_file_size_.put(field, current_length + (long) retArr.length);
    }

    static class FieldSizeData {
        public String field_;
        public long size_;
    }

    public ArrayList<FieldSizeData> prepare(ArrayList<Text> values) {

        ArrayList<Integer> used = new ArrayList<Integer>();

        HashMap<String, Long> fieldSize = new HashMap<String, Long>();
        for (int i = 0; i < values.size(); ++i) {
            Text value = values.get(i);
            String[] strs = value.toString().split("\t");
            if (strs.length != 4) {
                used.add(i);
                continue;
            }

            long len = (long) strs[2].length();
            String key = strs[1];
            if (fieldSize.containsKey(key)) {
                len += fieldSize.get(key);
            }

            fieldSize.put(key, len);
        }
        ReduceGroupData.clearIndex(values, used);

        ArrayList<FieldSizeData> ret = new ArrayList<FieldSizeData>();
        Iterator<String> itr = fieldSize.keySet().iterator();
        while (itr.hasNext()) {
            FieldSizeData fsd = new FieldSizeData();
            fsd.field_ = itr.next();
            fsd.size_ = fieldSize.get(fsd.field_);
            ret.add(fsd);
        }

        return ret;
    }

    private void write_cache(FSDataOutputStream out, String word, byte[] data) {
        try {
            byte[] buf = word.getBytes();
            out.writeInt(buf.length);
            out.write(buf);

            out.writeInt(data.length);
            out.write(data);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private InvertedIndex.Builder read_cache(FileSystem cache_file, Path cache_path_c) {
        InvertedIndex.Builder index_build = InvertedIndex.newBuilder();

        FSDataInputStream in = null;
        try {
            in = cache_file.open(cache_path_c);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return index_build;
        }

        while (true) {
            String word = null;
            byte[] buf = null;
            try {
                int len = in.readInt();
                buf = new byte[len];
                in.read(buf);
                word = new String(buf);

                len = in.readInt();
                buf = new byte[len];
                in.read(buf);
            } catch (Exception e) {
                break;
            }

            if (word != null && buf != null) {
                try {
                    DocIdList docid_list = DocIdList.parseFrom(buf);
                    index_build.getMutableIndex().put(word, docid_list);
                } catch (InvalidProtocolBufferException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

        if (in != null) {
            try {
                in.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        return index_build;
    }

    public void GetDocIdListStr(DocIdList build, StringBuffer ss) {

        for (int i = 1; i < build.getDocIdsCount(); i++) {
            if (i == 1) {
                ss.append("\t");
            } else {
                ss.append(";");
            }
            ss.append(build.getDocIds(i).getDocId()).append(",").append(build.getDocIds(i).getRowIndex());
        }
    }

    public DocIdList SortDocIdList(DocIdList.Builder doc_list_build, long pv) {
        int docid_count = doc_list_build.getDocIdsCount();
        int count = 0;
        long[] docids = new long[docid_count];
        Map docid_rowindex = new HashMap();
        DocIdList.Builder list = DocIdList.newBuilder();
        for (int i = 0; i < docid_count; i++) {
            docids[i] = doc_list_build.getDocIds(i).getDocId();
            // docids_old[i] = docids[i];
            if (!docid_rowindex.containsKey(docids[i])) {
                Vector vec = new Vector();
                vec.add(doc_list_build.getDocIds(i).getRowIndex());
                docid_rowindex.put(docids[i], vec);
            } else {
                ((Vector) docid_rowindex.get(docids[i])).add(doc_list_build.getDocIds(i).getRowIndex());
            }
        }
        Arrays.sort(docids);

        // Add Pv
        DocId.Builder docid = DocId.newBuilder();

        docid.setDocId(0);
        docid.setRowIndex((int) pv);
        list.addDocIds(docid.build());

        long last = -1;
        int last_count = 0;
        for (int i = 0; i < docid_count; i++) {
            docid = DocId.newBuilder();
            if (i == 0) {
                docid.setDocId(docids[i]);
            } else {
                docid.setDocId(docids[i] - docids[i - 1]);
            }
            if (last == docids[i]) {
                last_count++;
            } else {
                last_count = 0;
            }
            last = docids[i];
            docid.setRowIndex((int) ((Vector) docid_rowindex.get(last)).get(last_count));
            list.addDocIds(docid.build());
        }
        return list.build();
    }

}