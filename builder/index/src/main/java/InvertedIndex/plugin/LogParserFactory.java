package InvertedIndex.plugin;

import InvertedIndex.LogParser;
import org.apache.hadoop.conf.Configuration;

import java.util.Set;

/**
 * Created by liwei on 6/8/16.
 */
public class LogParserFactory {

    private void init(Configuration conf, CommonLogParser p) {
        String fields = new String();
        for (String f : p.fields()) {
            if (!fields.isEmpty()) {
                fields += ",";
            }
            fields += f;
        }
        System.err.println("fileds:" + fields);
        conf.set("fields", fields);
    }

    public void Init(Configuration conf) {
        String json_str = conf.get("json_conf");
        if (json_str == null || json_str.isEmpty()) {
            System.err.print("json_conf is null");
            return;
        }
        CommonLogParser p = new CommonLogParser(conf, json_str, false);
        init(conf, p);
        conf.set("filterfiles", p.Filterfiles());
    }

    public LogParser Create(Configuration conf) {
        LogParser logparser = null;
        String log_name = conf.get("log_name", new String("common"));
        String json_str = conf.get("json_conf");
        //json_str = Util.Base64DecoderStr(json_str, false);
        CommonLogParser p = new CommonLogParser(conf, json_str, true);
        p.set_debug(conf.getInt("_debug_", 0));
        init(conf, p);
        logparser = p;

        return logparser;
    }
}
