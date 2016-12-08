package InvertedIndex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.io.Text;

import proto.PoseidonIf.DocId;
import proto.PoseidonIf.DocIdList;

public class ReduceGroupData {

    static class MetaData {
        public DocIdList.Builder docid_list_build_ = DocIdList.newBuilder();
        public long pv_ = 0;
    }

    static class Result {
        //             word    data
        public HashMap<String, MetaData> data_ = new HashMap<String, MetaData>();
    }

    static public void clearIndex(ArrayList<Text> values, ArrayList<Integer> used) {
        for (int i = used.size() - 1; i >= 0; --i)
            values.remove(used.get(i));
    }

    static public Result runGroup(ArrayList<Text> values, String field) {

        ArrayList<Integer> used = new ArrayList<Integer>();

        Result result = new Result();
        //String flag = "\t"+field+"\t";
        for (int j = 0; j < values.size(); ++j) {
            //if ( val.find(flag)==-1 )
            //	continue;
            String[] strs = values.get(j).toString().trim().split("\t");
            if (strs.length != 4)
                continue;

            if (!field.equals(strs[1]))
                continue;

            used.add(j);

            // token\tfield\tdoiclist\tpv
            //this.add(strs[1], strs[0], strs[2], Integer.parseInt(strs[3]));
            String word = strs[0];
            String docids_str = strs[2];
            long current_len = Long.parseLong(strs[3]);

            if (!result.data_.containsKey(word)) {
                result.data_.put(word, new MetaData());
            }
            MetaData md = result.data_.get(word);
            md.pv_ += current_len;

            if (!isInvalidData(md.pv_, current_len, md.docid_list_build_)) {
                addDocidList(docids_str, md.docid_list_build_);
            }
        }

        clearIndex(values, used);

        return result;
    }

    static public MetaData runWord(ArrayList<Text> values, String field, String word) {

        ArrayList<Integer> used = new ArrayList<Integer>();
        MetaData md = new MetaData();

        //String flag1 = word+"\t";
        //String flag2 = "\t"+field+"\t";

        for (int j = 0; j < values.size(); ++j) {
            //if ( val.find(flag1)==-1 || val.find(flag2)==-1 )
            //	continue;
            String[] strs = values.get(j).toString().trim().split("\t");
            if (strs.length != 4)
                continue;

            if (!field.equals(strs[1]) || !word.equals(strs[0]))
                continue;

            used.add(j);

            //this.add(strs[1], strs[0], strs[2], Integer.parseInt(strs[3]));
            String docids_str = strs[2];
            long current_len = Long.parseLong(strs[3]);
            md.pv_ += current_len;

            if (!isInvalidData(md.pv_, current_len, md.docid_list_build_)) {
                addDocidList(docids_str, md.docid_list_build_);
            }
        }

        clearIndex(values, used);

        return md;
    }

    static public boolean isInvalidData(long pv, long current_len, DocIdList.Builder docid_list_build_) {

        int count = docid_list_build_.getDocIdsCount();
        if (pv > 1000 * 10000) {
            if (count > 100 * 10000) {
                DocId docid = docid_list_build_.getDocIds(0);
                long id = docid.getDocId();
                int index = docid.getRowIndex();
                docid = null;
                docid_list_build_.clearDocIds();

                DocId.Builder new_docid_build = DocId.newBuilder();
                new_docid_build.setDocId(id);
                new_docid_build.setRowIndex(index);
                docid_list_build_.addDocIds(new_docid_build.build());
                count = 1;
            }
            if (current_len > 1000)
                return true;
        }

        if (count > 100 * 10000)
            return true;
        return false;
    }

    static public int addDocidList(String docids_str, DocIdList.Builder docid_list_build_) {

        int count = docid_list_build_.getDocIdsCount();
        String[] docids = docids_str.split(";");
        long base = 0;
        for (int i = 0; i < docids.length; i++) {
            String docidstr = docids[i].trim();
            if (docidstr.isEmpty()) {
                continue;
            }
            String[] cols = docidstr.split(",");
            if (cols.length != 2)
                continue;
            DocId.Builder docid_build = DocId.newBuilder();
            base += Long.parseLong(cols[0]);
            int row_index = Integer.parseInt(cols[1]);
            docid_build.setDocId(base);
            docid_build.setRowIndex(row_index);
            docid_list_build_.addDocIds(docid_build.build());

            count++;
            if (count >= 100 * 10000) {
                break;
            }
        }

        return count;
    }

    static public ArrayList<String> getWords(ArrayList<Text> values, String field) {
        HashSet<String> words1 = new HashSet<String>();

        for (Text value : values) {
            String[] strs = value.toString().split("\t");
            if (strs.length != 4 || !field.equals(strs[1]))
                continue;

            words1.add(strs[0]);
        }

        ArrayList<String> words2 = new ArrayList<String>();
        for (String w : words1) {
            words2.add(w);
        }

        return words2;
    }
}
