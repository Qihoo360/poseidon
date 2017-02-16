import InvertedIndex.InvertedIndexGenerateReducer;
import InvertedIndex.InvertedIndexGenerateReducer.WordMemoryList;
import org.apache.hadoop.io.Text;
import proto.PoseidonIf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by lenovo on 2017/2/8.
 */
public class WordMemoryListTest {

    public static void main(String[] args) {
        List<Text> list = new ArrayList<Text>();
        list.add(new Text("s1\tf1\t1,50;1,38;1,45;0,89;1,70;2,19;0,29;3,41\t8"));
        list.add(new Text("s1\tf1\t3333,123\t1"));
        list.add(new Text("s1\tf1\t3343,124\t1"));

        Map<String, Map<String, WordMemoryList>> resultMap =
                InvertedIndexGenerateReducer.buildReduceResultMap(list);

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
                InvertedIndexGenerateReducer.GetDocIdListStr(curDocIdList, curBuf, 0);
                curBuf.append("\t").append(curMd.pv).append("\n");
                System.out.println(curBuf.toString());
            }
        }
    }

    public static void GetDocIdListStr(PoseidonIf.DocIdList build, StringBuffer ss) {

        for (int idx = 1; idx < build.getDocIdsCount(); idx++) {
            ss.append(";");
            ss.append(build.getDocIds(idx).getDocId()).append(",").append(build.getDocIds(idx).getRowIndex());
        }
    }
}
