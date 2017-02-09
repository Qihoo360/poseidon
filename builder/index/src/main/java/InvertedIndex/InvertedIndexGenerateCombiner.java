package InvertedIndex;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import InvertedIndex.InvertedIndexGenerateReducer.WordMemoryList;
import proto.PoseidonIf;

public class InvertedIndexGenerateCombiner extends Reducer<Text, Text, Text, Text> {

    //	private IndexGzMetaOutputFormat  multipleOutputs;
    private int save_line_count_per_map_;
    private int hash_num_per_indexgzmeta_;

    //private int count = 0;
    @Override
    protected void setup(Context context
    ) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        save_line_count_per_map_ = conf.getInt("save_line_count_per_map", 1000000);
        hash_num_per_indexgzmeta_ = conf.getInt("hash_num_per_indexgzmeta", 200);
        //System.err.println("save_line_count_per_map:" + save_line_count_per_map_);
        //System.err.println("hash_num_per_indexgzmeta:" + hash_num_per_indexgzmeta_);
    }

    @Override
    protected void cleanup(Context context
    ) throws IOException, InterruptedException {
        //multipleOutputs.close();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        Map<String, Map<String, WordMemoryList>> resultMap =
                InvertedIndexGenerateReducer.buildReduceResultMap(values);

        // 最后输出
        for(Map.Entry<String, Map<String, WordMemoryList>> entry : resultMap.entrySet()) {
            String curField = entry.getKey();
            for(Map.Entry<String, WordMemoryList> metaEntry : entry.getValue().entrySet()) {
                String curWord = metaEntry.getKey();
                WordMemoryList curMd = metaEntry.getValue();
                curMd.sort();

                StringBuffer curBuf = new StringBuffer();
                curBuf.append(curWord).append("\t").append(curField);
                PoseidonIf.DocIdList curDocIdList = curMd.getDocIdList(false);
                InvertedIndexGenerateReducer.GetDocIdListStr(curDocIdList, curBuf);
                curBuf.append("\t").append(curMd.pv).append("\n");
                // save middle
                context.write(key, new Text(curBuf.toString()));
            }
        }
    }

    protected void reduceOld(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        Map<String, Vector> token_type_docids = new HashMap(); //token"\t"ttype key
        Map<String, Map> token_type_offsets = new HashMap(); //token"\t"ttype key

        Map token_type_docids_count = new HashMap(); //token"\t"type key

        for (Text value : values) {
            //String value = token "\t" type +"\t" + docid +"," + line_offset + "\t" + count;
            String val = value.toString();

            // 查 \t 应该从后往前，token中可能包含\t
            String fields[] = new String[4];// token type  docidlist  count
            int count_pos = val.lastIndexOf("\t");
            if (count_pos <= 0) continue;
            fields[3] = val.substring(count_pos + 1);
            if (Integer.parseInt(fields[3]) > save_line_count_per_map_) {
                if (context != null)
                    context.write(key, value);
                continue;
            }

            for (int i = 2; i >= 0; i--) {
                int type_pos = val.lastIndexOf("\t", count_pos - 1);
                if (i == 0 || type_pos < 0) {
                    fields[i] = val.substring(0, count_pos);
                    break;
                }
                fields[i] = val.substring(type_pos + 1, count_pos);
                count_pos = type_pos;
            }
            if (fields[0].isEmpty()) {
                System.err.println(value.toString() + "fffff");
                continue;
            }
            fields[0] = fields[0].replace('\t', ' ');
            //System.err.println( fields[0]+", "+fields[1]+", "+fields[2]+", "+fields[3] );

            String map_key = fields[0] + "\t" + fields[1];
            if (!token_type_docids.containsKey(map_key)) {
                token_type_docids.put(map_key, new Vector());
                token_type_offsets.put(map_key, new HashMap());
                token_type_docids_count.put(map_key, 0);
                ;
            }
            int count1 = (int) token_type_docids_count.get(map_key);

            token_type_docids_count.put(map_key, count1 + Integer.parseInt(fields[3]));

            if (count1 > save_line_count_per_map_) {
                continue;
            }
            ParseDocidsDifference(map_key, fields[2].trim(), token_type_docids, token_type_offsets);
        }

        //count++;
        Set map_keys = token_type_docids_count.keySet();
        Iterator map_keys_iter = map_keys.iterator();
        while (map_keys_iter.hasNext()) {
            String token_type = (String) map_keys_iter.next();
            StringBuffer ss = new StringBuffer();
            String docids = DocidsDifference(token_type_docids.get(token_type), token_type_offsets.get(token_type));
            ss.append(token_type).append("\t").append(docids).append("\t").append((int) (token_type_docids_count.get(token_type)));
            context.write(new Text(key.toString()), new Text(ss.toString()));

        }
        token_type_docids = null;
        token_type_docids_count = null;
    }

    public String DocidsDifference(Vector docids_vec, Map offsets_map) {
        Object[] docids = docids_vec.toArray();
        Arrays.sort(docids);
        StringBuffer ss = new StringBuffer();
        long last = -2;
        int last_count = 0;
        for (int i = 0; i < docids.length; i++) {
            if (last == (long) docids[i]) {
                last_count++;
            } else {
                last_count = 0;
            }
            if (i == 0) {
                ss.append(docids[i]).append(",").append(((Vector) offsets_map.get(docids[i])).get(last_count));
            } else {
                ss.append(";").append((long) docids[i] - (long) docids[i - 1]).append(",").append(((Vector) offsets_map.get(docids[i])).get(last_count));
            }
            last = (long) docids[i];
        }
        return ss.toString();
    }

    public void ParseDocidsDifference(String key, String val, Map<String, Vector> token_type_docids, Map<String, Map> token_type_offsets) {

        String[] docid_offsets = val.split(";");
        if (docid_offsets == null) {
            return;
        }
        int pos = docid_offsets[0].indexOf(',');
        if (pos <= 0) {
            return;
        }
        long base = Long.parseLong(docid_offsets[0].substring(0, pos));
        int offset = Integer.parseInt(docid_offsets[0].substring(pos + 1));
        Vector docids = token_type_docids.get(key);
        Map offsets_map = token_type_offsets.get(key);
        Vector offsets = null;
        if (!offsets_map.containsKey(base)) {
            offsets = new Vector();
            offsets_map.put(base, offsets);
        } else {
            offsets = (Vector) offsets_map.get(base);
        }
        docids.add(base);
        offsets.add(offset);
        int total_count = docids.size();
        for (int i = 1; i < docid_offsets.length; i++) {
            if (docid_offsets[i].length() < 3) {
                return;
            }
            pos = docid_offsets[i].indexOf(',');
            if (pos <= 0) {
                return;
            }
            base = Long.parseLong(docid_offsets[i].substring(0, pos)) + base;
            offset = Integer.parseInt(docid_offsets[i].substring(pos + 1));
            docids.add(base);
            if (!offsets_map.containsKey(base)) {
                offsets = new Vector();
                offsets_map.put(base, offsets);
            } else {
                offsets = (Vector) offsets_map.get(base);
            }
            offsets.add(offset);
        }
    }
}