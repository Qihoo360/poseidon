package InvertedIndex.plugin.Function;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by i-xuchongpeng on 2016/8/11.
 */
public class UrlEncode implements Function {
    public Set<String> Process(String input) {
        Set<String> set = new HashSet<String>();
        set.add(java.net.URLEncoder.encode(input).toLowerCase());
        return set;
    }

    public Set<String> Process(Set<String> input) {
        Set<String> set = new HashSet<String>();
        for (String s : input) {
            set.add(java.net.URLEncoder.encode(s).toLowerCase());
        }
        return set;
    }

}
