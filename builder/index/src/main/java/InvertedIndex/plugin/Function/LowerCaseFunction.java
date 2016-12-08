package InvertedIndex.plugin.Function;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by i-xuchongpeng on 2016/8/11.
 */
public class LowerCaseFunction implements Function {
    public Set<String> Process(String input) {
        Set<String> set = new HashSet<String>();
        set.add(input.trim().toLowerCase());
        return set;
    }

    public Set<String> Process(Set<String> input) {
        Set<String> set = new HashSet<String>();
        for (String s : input) {
            set.add(s.trim().toLowerCase());
        }
        return set;
    }
}
