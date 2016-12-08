package InvertedIndex.plugin.Function;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by i-xuchongpeng on 2016/8/11.
 */
public class UrlDecodeFunction implements Function {
    public Set<String> Process(String input) {
        Set<String> set = new HashSet<String>();
        try {
            set.add(java.net.URLDecoder.decode(input));
        } catch (Exception e) {
        }
        return set;
    }

    public Set<String> Process(Set<String> input) {
        Set<String> set = new HashSet<String>();
        for (String s : input) {
            try {
                set.add(java.net.URLDecoder.decode(s));
            } catch (Exception e) {
            }
        }
        return set;
    }
}
