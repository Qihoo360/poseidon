package InvertedIndex.plugin.Function;

import InvertedIndex.plugin.Util;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by i-xuchongpeng on 2016/8/11.
 */
public class PathFunction implements Function {
    public Set<String> Process(String input) {
        Set<String> set = new HashSet<String>();
        Util.ParsePath(input, set);
        return set;
    }

    public Set<String> Process(Set<String> input) {
        Set<String> set = new HashSet<String>();
        for (String s : input) {
            Util.ParsePath(s, set);
        }
        return set;
    }
}
