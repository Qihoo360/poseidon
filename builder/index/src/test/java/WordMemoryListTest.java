import InvertedIndex.InvertedIndexGenerateReducer.WordMemoryList;
import proto.PoseidonIf;

/**
 * Created by lenovo on 2017/2/8.
 */
public class WordMemoryListTest {

    public static void main(String[] args) {
        WordMemoryList wml = new WordMemoryList(5);
        wml.addDocIds("1,1;1,1;1,1;1,1;1,2", 5);
        wml.addDocIds("8,1;1,1;1,3", 3);

        PoseidonIf.DocIdList docIdList = wml.getDocIdList();

        StringBuffer sb = new StringBuffer();
        GetDocIdListStr(docIdList, sb);
        System.out.println(sb.toString());
    }

    public static void GetDocIdListStr(PoseidonIf.DocIdList build, StringBuffer ss) {

        for (int idx = 1; idx < build.getDocIdsCount(); idx++) {
            ss.append(";");
            ss.append(build.getDocIds(idx).getDocId()).append(",").append(build.getDocIds(idx).getRowIndex());
        }
    }
}
