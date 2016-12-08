package InvertedIndex.plugin.Function;

import InvertedIndex.plugin.CommonLogParser;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Set;
import java.util.Vector;

/**
 * Created by i-xuchongpeng on 2016/8/11.
 */
public class TokenParser {
    private String key_;
    private String alias_;
    private int index_;
    private CommonLogParser commonLogParser_;

    private Vector<Function> functions_ = new Vector<>();

    public TokenParser(CommonLogParser p, String key) {
        commonLogParser_ = p;
        key_ = key;
        alias_ = key;
    }

    public void Add(String func, String para) {
        System.err.println(key_ + " will add  [" + func + "] " + para);
        Function function = null;
        if (func.compareTo("urldecode") == 0) {
            function = new UrlDecodeFunction();
        } else if (func.compareTo("urlencode") == 0) {
            function = new UrlEncodeFunction();
        } else if (func.compareTo("base64decode") == 0) {
            function = new Base64DecodeFunction();
        } else if (func.compareTo("url") == 0) {
            function = new UrlFunction();
        } else if (func.compareTo("path") == 0) {
            function = new PathFunction();
        } else if (func.compareTo("pathWithUrlencode") == 0) {
            function = new PathWithUrlencodeFunction();
        } else if (func.compareTo("ip") == 0) {
            function = new IpFunction();
        } else if (func.compareTo("keyword") == 0) {
            function = new KeywordFunction();
        } else if (func.compareTo("split") == 0) {
            function = new SplitFunction(para);
        } else if (func.compareTo("regexcheck") == 0) {
            function = new RegexCheck(para);
        } else if (func.compareTo("tokenfilter") == 0) {
            function = new FilterFunction(commonLogParser_.GetFilter(para));
        } else if (func.compareTo("text") == 0) {
            function = new TextFunction();
        }
        if (function != null) {
            functions_.add(function);
        }
    }

    public void set_alias(String key) {
        alias_ = key;
    }

    public void set_index(int index) {
        index_ = index;
    }

    public String key() {
        return key_;
    }

    public String alias() {
        return alias_;
    }

    public int index() {
        return index_;
    }

    public Set<String> Process(String[] list) {
        if (this.index() + 1 > list.length) {
            return null;
        }
        return Process(list[this.index()]);
    }

    public Set<String> Process(JSONObject json) {
        String[] list = key_.split("\\.");
        JSONObject last = json;
        JSONObject current = json;
        String value = null;
        boolean found = true;
        Object o;
        try {
            for (int i = 0; i < list.length - 1; i++) {
                if (last.has(list[i])) {
                    current = last.getJSONObject(list[i]);
                    last = current;
                    found = true;
                } else {
                    found = false;
                    break;
                }
            }
            if (found && last.has(list[list.length - 1])) {
                o = last.get(list[list.length - 1]);
                if (o instanceof JSONObject) {
                    value = ((JSONObject) o).toString();
                } else if (o instanceof String) {
                    value = (String) o;
                } else {
                    value = o.toString();
                }
                return Process(value);
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }

        return null;
    }

    public Set<String> Process(String value) {
        Set<String> last_output_set = null;
        Set<String> current_output_set = null;
        for (int i = 0; i < functions_.size(); i++) {
            if (i == 0) {
                current_output_set = functions_.get(i).Process(value);
            } else {
                current_output_set = functions_.get(i).Process(last_output_set);
            }
            last_output_set = current_output_set;
        }
        return last_output_set;
    }
}
