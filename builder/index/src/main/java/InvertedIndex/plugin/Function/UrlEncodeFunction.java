package InvertedIndex.plugin.Function;

import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by i-xuchongpeng on 2016/8/11.
 */
public class UrlEncodeFunction implements Function {
    protected String process(String input) {
        try {
            return URLEncoder.encode(input, "utf-8");
        } catch (Exception e) {
            try {
                return URLEncoder.encode(input, "gb18030");
            } catch (Exception e1) {
            }
        }
        return input;
    }

    public Set<String> Process(String input) {
        Set<String> set = new HashSet<String>();
        String str = process(input);
        set.add(str);
        return set;
    }

    public Set<String> Process(Set<String> input) {
        Set<String> set = new HashSet<String>();
        for (String s : input) {
            String str = process(s);
            set.add(str);
        }
        return set;
    }
}
