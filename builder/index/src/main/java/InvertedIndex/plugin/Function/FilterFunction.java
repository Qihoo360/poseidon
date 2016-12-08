package InvertedIndex.plugin.Function;

//import InvertedIndex.plugin.Function.*;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by i-xuchongpeng on 2016/8/11.
 */
public class FilterFunction implements Function {
    private TokenFilter filter_;

    public FilterFunction(TokenFilter filters) {
        filter_ = filters;
    }

    public Set<String> Process(String input) {
        Set<String> set = new HashSet<>();
        if (filter_.PassBy(input)) set.add(input);
        return set;
    }

    public Set<String> Process(Set<String> input) {
        Set<String> set = new HashSet<>();
        for (String s : input) {
            if (filter_.PassBy(s)) set.add(s);
        }
        return set;
    }
}
