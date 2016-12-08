package InvertedIndex.plugin.Function;

import InvertedIndex.plugin.Util;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by i-xuchongpeng on 2016/8/11.
 */
public class IpFunction implements Function {
    public Set<String> Process(String input) {
        Set<String> set = new HashSet<String>();
        set.addAll(Util.ParseIp(input));
        return set;
    }

    public Set<String> Process(Set<String> input) {
        Set<String> set = new HashSet<String>();
        for (String s : input) {
            set.addAll(Util.ParseIp(s));
        }
        return set;
    }
}
