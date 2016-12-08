package InvertedIndex.plugin.Function;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by i-xuchongpeng on 2016/8/11.
 */
public class SplitFunction implements Function {
    private String sep_;

    public SplitFunction(String sep) {
        sep_ = sep;
    }

    public Set<String> Process(String input) {
        Set<String> set = new HashSet<String>();
        String[] vals = input.split(sep_);
        for (int i = 0; i < vals.length; i++) {
            set.add(vals[i]);
        }
        return set;
    }

    public Set<String> Process(Set<String> input) {
        Set<String> set = new HashSet<String>();
        for (String s : input) {
            String[] vals = s.split(sep_);
            for (int i = 0; i < vals.length; i++) {
                set.add(vals[i]);
            }
        }
        return set;
    }
}
