package InvertedIndex.plugin.Function;

import InvertedIndex.plugin.Util;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by i-xuchongpeng on 2016/8/11.
 */
public class Base64DecodeFunction implements Function {
    public Set<String> Process(String input) {
        Set<String> set = new HashSet<String>();
        set.add(Util.Base64DecoderStr(input, false));
        return set;
    }

    public Set<String> Process(Set<String> input) {
        Set<String> set = new HashSet<String>();
        for (String s : input) {
            set.add(Util.Base64DecoderStr(s, false));
        }
        return set;
    }
}
