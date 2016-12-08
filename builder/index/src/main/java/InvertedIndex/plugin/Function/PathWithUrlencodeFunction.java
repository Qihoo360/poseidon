package InvertedIndex.plugin.Function;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by i-xuchongpeng on 2016/8/11.
 */
public class PathWithUrlencodeFunction implements Function {
    public Set<String> Process(String input) {
        Set<String> set = new HashSet<String>();
        if (input == null || input.isEmpty()) {
            return set;
        }
        String fpath = input.trim().toLowerCase();
        if (fpath.isEmpty()) {
            return set;
        }

        if (!fpath.isEmpty()) {
            String[] fpath_parts = fpath.split("%5c");
            for (int i = 0; i < fpath_parts.length; i++) {
                if (fpath_parts[i].isEmpty()) {
                    continue;
                }
                if (!set.contains(fpath_parts[i])) {
                    set.add(fpath_parts[i]);
                }
            }
            if (!set.contains(fpath)) {
                set.add(fpath);
            }
        }
        return set;
    }

    public Set<String> Process(Set<String> input) {

        Set<String> set = new HashSet<String>();
        for (String s : input) {
            if (s == null || s.isEmpty()) {
                continue;
            }
            String fpath = s.trim().toLowerCase();
            if (fpath.isEmpty()) {
                continue;
            }
            if (!fpath.isEmpty()) {
                String[] fpath_parts = fpath.split("%5c");
                for (int i = 0; i < fpath_parts.length; i++) {
                    if (fpath_parts[i].isEmpty()) {
                        continue;
                    }
                    if (!set.contains(fpath_parts[i])) {
                        set.add(fpath_parts[i]);
                    }
                }
                if (!set.contains(fpath)) {
                    set.add(fpath);
                }
            }
        }

        return set;
    }
}
