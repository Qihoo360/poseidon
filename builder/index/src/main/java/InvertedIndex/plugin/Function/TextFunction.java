package InvertedIndex.plugin.Function;

import InvertedIndex.plugin.Util;

import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;


/**
 * Created by liwei on 9/21/16.
 */
public class TextFunction implements Function {

    private IKSegmenter iksegmenter_ = new IKSegmenter(new StringReader(""), true);

    public Set<String> Process(String input) {
        Set<String> set = new HashSet<String>();
        try {
            segmentDumpstr(input, set);
        } catch (Exception e) {
        }
        return set;
    }

    public Set<String> Process(Set<String> input) {
        Set<String> set = new HashSet<String>();
        for (String s : input) {
            try {
                segmentDumpstr(s, set);
            } catch (Exception e) {
            }
        }
        return set;
    }

    private boolean segmentNeedOutput(String seg) {
        if (seg.isEmpty()) {
            return false;
        }
        if (Util.IsChinese(seg)) {
            if (2 > seg.length())
                return false;
        } else if (Util.IsDigit(seg)) {
            if (5 > seg.length())
                return false;
        } else if (Util.IsHexadecimal(seg)) {
            if (6 > seg.length())
                return false;
        } else if (4 > seg.length()) {
            return false;
        } else if (128 < seg.length()) {
            return false;
        } else if (Util.IsUnReadable(seg)) {
            return false;
        }
        return true;
    }

    public Set segmentDumpstr(String str, Set<String> segs) {
        iksegmenter_.reset(new StringReader(str));
        try {
            Lexeme word = null;
            while ((word = iksegmenter_.next()) != null) {
                String seg = word.getLexemeText();
                if (seg == null)
                    continue;
                seg = seg.trim();

                // 组合起来，之后判断和原串长度相似需要输出该完整串
                //result.append( seg );
                // 汉字长度要>=2，字母其他要>=4
                if (!segmentNeedOutput(seg)) {
                    continue;
                }
                segs.add(seg.toLowerCase());
            }
        } catch (Exception ex) {
            //throw new RuntimeException(ex);
            ex.printStackTrace();
        }

        return segs;
    }

}

