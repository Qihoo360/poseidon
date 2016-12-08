package InvertedIndex.plugin.Function;

import InvertedIndex.plugin.Util;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by i-xuchongpeng on 2016/8/11.
 */
public class RegexCheck implements Function {
    private String pattern = null;

    public RegexCheck(String pattern) {
        if (pattern != null) {
            this.pattern = Util.Base64DecoderStr(pattern, false);
        }
    }

    protected String process(String input) {
        if (this.pattern == null)
            return input;
        if (input.matches(pattern))
            return input;
        return null;
    }

    public Set<String> Process(String input) {
        Set<String> result = new HashSet<String>();
        String s = process(input);
        if (s != null)
            result.add(s);
        return result;
    }

    public Set<String> Process(Set<String> inputs) {
        Set<String> result = new HashSet<String>();
        for (String input : inputs) {
            String s = process(input);
            if (s != null)
                result.add(s);
        }
        return result;
    }
}
